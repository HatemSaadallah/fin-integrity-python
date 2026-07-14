# fin-integrity (Python)

**Reconciliation-as-you-code.** Capture your payment-processor events _and_ your internal ledger entries from your backend, and fin·integrity continuously matches them — surfacing missing entries, duplicates, missing refunds, and amount/currency mismatches as incidents, in real time.

- Zero runtime dependencies (stdlib only), Python 3.8+
- Async, batched, non-blocking capture with a background flush thread
- **Fail-open** — the SDK never raises into your money path
- Integer minor-units money model; deterministic, retry-safe idempotency keys

> Framework integrations: **Django** → [`fin-integrity-django`](https://github.com/HatemSaadallah/fin-integrity-django) · **FastAPI** → [`fin-integrity-fastapi`](https://github.com/HatemSaadallah/fin-integrity-fastapi).

## Install

```bash
pip install fin-integrity
```

## Quickstart

```python
from fin_integrity import init

fi = init(api_key="fi_sk_live_...")   # or set FIN_INTEGRITY_KEY

# processor side (what actually moved)
fi.processor.record(
    type="payment", source="stripe", reference="order_10432",
    external_id="ch_123", amount_minor=4999, currency="usd", status="succeeded",
)

# ledger side (your books) — same reference ties them together
fi.ledger.record(
    type="payment", reference="order_10432",
    external_id="journal_5001", amount_minor=4999, currency="usd",
)

fi.flush()   # force-send now (e.g. in a short-lived script or request)
```

fin·integrity matches the two by `reference` + `type`, then **compares** amount and currency — so a wrong amount surfaces as an incident instead of silently failing to match.

## API

- `init(**config) -> FinIntegrityClient` — create + store the singleton (also returned). `get_client()` returns it.
- `fi.processor.record(...)` / `fi.ledger.record(...)` — keyword args: `type` (`"payment"|"refund"`), `reference`, `external_id`, `amount_minor` (int, minor units), `currency`, and optional `source`, `status`, `occurred_at`, `direction`, `exponent`, `metadata`.
- `fi.capture(side=..., **fields)` — low-level escape hatch.
- `fi.flush()` — send queued events now. `fi.shutdown()` — drain and stop.
- `fi.inspect()` — captured envelopes (in `dry_run` or with a custom `transport`), for tests.

**Config** (all keyword): `api_key`, `endpoint`, `environment`, `idempotency` (`"deterministic"` | `"uuid"`), `batch_max_size`, `flush_interval`, `max_queue_size`, `retries`, `sample_rate` (default `1.0`), `before_send`, `debug`, `dry_run`, `on_error`, `transport`.

## Money

Integer **minor units** + ISO-4217 currency — never floats. `4999` = `$49.99`; `1000` = `¥1000` (JPY, zero-decimal); `10000` + `exponent=3` = `10.000 BHD`.

## Security

Send transaction metadata (ids, amounts, currency, status) — **never card numbers/PANs or secrets**. Use `before_send` to redact. Keys are server-side secrets.

## License

MIT © fin-integrity
