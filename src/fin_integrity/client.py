"""fin-integrity core client — reconciliation-as-you-code.

Capture payment-processor and ledger events and stream them to the fin-integrity
ingest API. Fail-open by design: the client never raises into your money path.
"""
from __future__ import annotations

import atexit
import hashlib
import json
import os
import random
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Optional

DEFAULT_ENDPOINT = "https://ingest.fin-integrity.com"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _to_iso(v: Any) -> str:
    if v is None:
        return _now_iso()
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(v)


class _Side:
    """Namespace exposing .record() for one side (processor / ledger)."""

    def __init__(self, client: "FinIntegrityClient", side: str) -> None:
        self._client = client
        self._side = side

    def record(
        self,
        *,
        type: str,
        reference: str,
        external_id: str,
        amount_minor: int,
        currency: str,
        source: Optional[str] = None,
        status: Optional[str] = None,
        occurred_at: Any = None,
        direction: Optional[str] = None,
        exponent: Optional[int] = None,
        metadata: Optional[dict] = None,
        fee_minor: Optional[int] = None,
        fee_currency: Optional[str] = None,
        trace_id: Optional[str] = None,
        payout_id: Optional[str] = None,
    ) -> None:
        self._client._record(
            self._side, type, reference, external_id, amount_minor, currency,
            source, status, occurred_at, direction, exponent, metadata,
            fee_minor, fee_currency, trace_id, payout_id,
        )

    def record_payout(
        self,
        *,
        external_id: str,
        amount_minor: int,
        currency: str,
        arrival_at: Any = None,
        trace_id: Optional[str] = None,
        occurred_at: Any = None,
        source: Optional[str] = None,
        status: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        """Capture a processor payout (processor -> bank). Stored separately;
        links to transactions via their payout_id."""
        self._client._record_payout(
            external_id, amount_minor, currency, arrival_at, trace_id,
            occurred_at, source, status, metadata,
        )


class FinIntegrityClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        *,
        endpoint: Optional[str] = None,
        environment: Optional[str] = None,
        idempotency: str = "deterministic",
        batch_max_size: int = 50,
        flush_interval: float = 2.0,
        max_queue_size: int = 1000,
        retries: int = 3,
        sample_rate: float = 1.0,
        before_send: Optional[Callable[[dict], Optional[dict]]] = None,
        debug: bool = False,
        dry_run: bool = False,
        on_error: Optional[Callable[[Exception], None]] = None,
        transport: Optional[Callable[[list], None]] = None,
    ) -> None:
        self.api_key = api_key or os.environ.get("FIN_INTEGRITY_KEY")
        self.dry_run = dry_run
        if not self.api_key and not dry_run and transport is None:
            raise ValueError(
                "fin-integrity: api_key is required (pass api_key or set FIN_INTEGRITY_KEY). "
                "Use dry_run=True to test without a key."
            )
        self.endpoint = (endpoint or os.environ.get("FIN_INTEGRITY_ENDPOINT") or DEFAULT_ENDPOINT).rstrip("/")
        self.environment = environment or os.environ.get("PYTHON_ENV") or "production"
        self.idempotency = idempotency
        self.batch_max_size = batch_max_size
        self.flush_interval = flush_interval
        self.max_queue_size = max_queue_size
        self.retries = retries
        self.sample_rate = sample_rate
        self.before_send = before_send
        self.debug = debug
        self.on_error = on_error or (lambda e: None)
        self._transport = transport
        self._queue: list = []
        self._dropped = 0
        self._sent: list = []
        self._lock = threading.Lock()
        self._closed = False

        self.processor = _Side(self, "processor")
        self.ledger = _Side(self, "ledger")

        self._schedule()
        atexit.register(self.flush)

    # ---- capture ---------------------------------------------------------
    def capture(self, *, side: str, **kwargs: Any) -> None:
        self._record(
            side, kwargs["type"], kwargs["reference"], kwargs["external_id"],
            kwargs["amount_minor"], kwargs["currency"], kwargs.get("source"),
            kwargs.get("status"), kwargs.get("occurred_at"), kwargs.get("direction"),
            kwargs.get("exponent"), kwargs.get("metadata"),
            kwargs.get("fee_minor"), kwargs.get("fee_currency"),
            kwargs.get("trace_id"), kwargs.get("payout_id"),
        )

    def _record(self, side, type, reference, external_id, amount_minor, currency,
                source, status, occurred_at, direction, exponent, metadata,
                fee_minor=None, fee_currency=None, trace_id=None, payout_id=None) -> None:
        try:
            if int(amount_minor) != amount_minor:
                raise ValueError("amount_minor must be an integer in minor units")
            env = {
                "schema_version": "1.0",
                "event_id": "fi_" + uuid.uuid4().hex,
                "idempotency_key": "",
                "side": side,
                "source": source or ("ledger.internal" if side == "ledger" else "custom"),
                "event_type": type,
                "reference": reference,
                "external_id": external_id,
                "amount": {"minor": str(int(amount_minor)), "currency": currency.lower()},
                "occurred_at": _to_iso(occurred_at),
                "captured_at": _now_iso(),
            }
            if exponent is not None:
                env["amount"]["exponent"] = exponent
            if fee_minor is not None:
                env["fee"] = {"minor": str(int(fee_minor)), "currency": (fee_currency or currency).lower()}
            if trace_id is not None:
                env["trace_id"] = trace_id
            if payout_id is not None:
                env["payout_id"] = payout_id
            if status is not None:
                env["status"] = status
            if direction is not None:
                env["direction"] = direction
            if metadata is not None:
                env["metadata"] = metadata
            env["idempotency_key"] = self._idem(env)
            self._enqueue(env)
        except Exception as e:  # fail-open — never raise into the caller
            self.on_error(e)

    def _record_payout(self, external_id, amount_minor, currency, arrival_at,
                       trace_id, occurred_at, source, status, metadata) -> None:
        try:
            if int(amount_minor) != amount_minor:
                raise ValueError("amount_minor must be an integer in minor units")
            env = {
                "schema_version": "1.0",
                "event_id": "fi_" + uuid.uuid4().hex,
                "idempotency_key": "",
                "side": "processor",
                "source": source or "custom",
                "event_type": "payout",
                "reference": external_id,
                "external_id": external_id,
                "amount": {"minor": str(int(amount_minor)), "currency": currency.lower()},
                "occurred_at": _to_iso(occurred_at),
                "captured_at": _now_iso(),
            }
            if trace_id is not None:
                env["trace_id"] = trace_id
            if arrival_at is not None:
                env["arrival_at"] = _to_iso(arrival_at)
            if status is not None:
                env["status"] = status
            if metadata is not None:
                env["metadata"] = metadata
            env["idempotency_key"] = self._idem(env)
            self._enqueue(env)
        except Exception as e:  # fail-open — never raise into the caller
            self.on_error(e)

    def _idem(self, env: dict) -> str:
        if self.idempotency == "uuid":
            return "fi_" + uuid.uuid4().hex
        basis = f"{env['source']}:{env['side']}:{env['external_id']}:{env['event_type']}"
        return "fi_" + hashlib.sha256(basis.encode()).hexdigest()[:40]

    def _enqueue(self, env: dict) -> None:
        if self.sample_rate < 1 and random.random() > self.sample_rate:
            return
        if self.before_send:
            env = self.before_send(env)
            if not env:
                return
        with self._lock:
            if len(self._queue) >= self.max_queue_size:
                self._queue.pop(0)
                self._dropped += 1
            self._queue.append(env)
            should_flush = len(self._queue) >= self.batch_max_size
        if should_flush:
            self.flush()

    # ---- delivery --------------------------------------------------------
    def flush(self) -> None:
        with self._lock:
            if not self._queue:
                return
            batch, self._queue = self._queue, []
            dropped, self._dropped = self._dropped, 0
        try:
            self._send(batch, dropped)
        except Exception as e:  # fail-open
            self.on_error(e)

    def _send(self, batch: list, dropped: int) -> None:
        if self._transport is not None:
            self._transport(batch)
            return
        if self.dry_run:
            self._sent.extend(batch)
            return
        body = json.dumps({"sent_at": _now_iso(), "dropped": dropped, "events": batch}).encode()
        url = self.endpoint + "/v1/events"
        attempt = 0
        while True:
            req = urllib.request.Request(
                url, data=body, method="POST",
                headers={
                    "content-type": "application/json",
                    "authorization": f"Bearer {self.api_key}",
                    "idempotency-key": batch[0]["idempotency_key"] if batch else "",
                },
            )
            try:
                with urllib.request.urlopen(req, timeout=10):
                    if self.debug:
                        print(f"[fin-integrity] delivered {len(batch)} event(s)")
                    return
            except urllib.error.HTTPError as e:
                if e.code == 429 or e.code >= 500:
                    if attempt >= self.retries:
                        raise
                    ra = e.headers.get("retry-after")
                    time.sleep(float(ra) if ra and ra.replace(".", "").isdigit() else min(2 ** attempt, 15))
                    attempt += 1
                    continue
                raise  # 4xx terminal
            except urllib.error.URLError:
                if attempt >= self.retries:
                    raise
                time.sleep(min(2 ** attempt, 15))
                attempt += 1

    def _schedule(self) -> None:
        self._timer = threading.Timer(self.flush_interval, self._tick)
        self._timer.daemon = True
        self._timer.start()

    def _tick(self) -> None:
        self.flush()
        if not self._closed:
            self._schedule()

    def shutdown(self) -> None:
        self._closed = True
        try:
            self._timer.cancel()
        except Exception:
            pass
        self.flush()

    def inspect(self) -> list:
        """Envelopes captured so far (dry_run / custom transport). Great for tests."""
        with self._lock:
            return list(self._sent) + list(self._queue)


_current: Optional[FinIntegrityClient] = None


def init(**kwargs: Any) -> FinIntegrityClient:
    """Create the client and store it as the module singleton (also returned)."""
    global _current
    _current = FinIntegrityClient(**kwargs)
    return _current


def get_client() -> FinIntegrityClient:
    if _current is None:
        raise RuntimeError("fin-integrity: call init() before get_client()")
    return _current
