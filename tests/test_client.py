import json
import unittest
from datetime import datetime, timezone
from unittest import mock

from fin_integrity import FinIntegrityClient, RejectedEventsError


class _FakeResp:
    """Minimal stand-in for the urlopen context manager."""

    def __init__(self, body: bytes) -> None:
        self._body = body

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _http_client(**kwargs):
    """A client on the real HTTP path (no dry_run, no custom transport)."""
    return FinIntegrityClient(api_key="fi_sk_test_x", **kwargs)


class TestClient(unittest.TestCase):
    def test_processor_envelope(self):
        sent = []
        fi = FinIntegrityClient(transport=lambda b: sent.extend(b))
        fi.processor.record(
            type="payment", source="stripe", reference="order_1",
            external_id="ch_1", amount_minor=4999, currency="USD",
        )
        fi.flush()
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0]["side"], "processor")
        self.assertEqual(sent[0]["amount"], {"minor": "4999", "currency": "usd"})
        self.assertTrue(sent[0]["idempotency_key"].startswith("fi_"))

    def test_deterministic_idempotency(self):
        fi = FinIntegrityClient(dry_run=True)
        for _ in range(2):
            fi.processor.record(
                type="payment", source="stripe", reference="o",
                external_id="ch_9", amount_minor=100, currency="usd",
            )
        events = fi.inspect()
        self.assertEqual(events[0]["idempotency_key"], events[1]["idempotency_key"])

    def test_non_integer_amount_fails_open(self):
        errors = []
        fi = FinIntegrityClient(dry_run=True, on_error=lambda e: errors.append(e))
        fi.processor.record(
            type="payment", reference="o", external_id="x",
            amount_minor=10.5, currency="usd",
        )
        self.assertEqual(len(errors), 1)
        self.assertEqual(len(fi.inspect()), 0)

    def test_ledger_dry_run(self):
        fi = FinIntegrityClient(dry_run=True)
        fi.ledger.record(type="payment", reference="o", external_id="je_1", amount_minor=100, currency="usd")
        self.assertEqual(fi.inspect()[0]["side"], "ledger")

    def test_fee_serializes_as_string_minor_dict(self):
        fi = FinIntegrityClient(dry_run=True)
        fi.processor.record(
            type="payment", source="stripe", reference="order_1",
            external_id="ch_1", amount_minor=4999, currency="USD",
            fee_minor=175, fee_currency="USD",
        )
        env = fi.inspect()[0]
        self.assertEqual(env["fee"], {"minor": "175", "currency": "usd"})

    def test_trace_and_payout_ids_present_when_passed(self):
        fi = FinIntegrityClient(dry_run=True)
        fi.processor.record(
            type="payment", source="stripe", reference="order_1",
            external_id="ch_1", amount_minor=4999, currency="usd",
            trace_id="trace_abc", payout_id="po_123",
        )
        env = fi.inspect()[0]
        self.assertEqual(env["trace_id"], "trace_abc")
        self.assertEqual(env["payout_id"], "po_123")

    def test_trace_and_payout_ids_absent_when_not_passed(self):
        fi = FinIntegrityClient(dry_run=True)
        fi.processor.record(
            type="payment", source="stripe", reference="order_1",
            external_id="ch_1", amount_minor=4999, currency="usd",
        )
        env = fi.inspect()[0]
        self.assertNotIn("fee", env)
        self.assertNotIn("trace_id", env)
        self.assertNotIn("payout_id", env)

    def test_record_payout_envelope(self):
        fi = FinIntegrityClient(dry_run=True)
        fi.processor.record_payout(
            external_id="po_123", amount_minor=100000, currency="USD",
            trace_id="trace_xyz",
        )
        env = fi.inspect()[0]
        self.assertEqual(env["event_type"], "payout")
        self.assertEqual(env["side"], "processor")
        self.assertEqual(env["reference"], "po_123")
        self.assertEqual(env["external_id"], "po_123")
        self.assertEqual(env["amount"], {"minor": "100000", "currency": "usd"})
        self.assertEqual(env["trace_id"], "trace_xyz")

    def test_dispute_is_a_valid_money_movement_type(self):
        fi = FinIntegrityClient(dry_run=True)
        fi.processor.record(
            type="dispute", source="stripe", reference="order_1",
            external_id="dp_1", amount_minor=4999, currency="usd",
            status="lost", parent_external_id="ch_1",
        )
        env = fi.inspect()[0]
        self.assertEqual(env["event_type"], "dispute")
        self.assertEqual(env["status"], "lost")
        self.assertEqual(env["side"], "processor")
        self.assertEqual(env["amount"], {"minor": "4999", "currency": "usd"})

    def test_subscription_and_parent_ids_present_when_passed(self):
        fi = FinIntegrityClient(dry_run=True)
        fi.processor.record(
            type="refund", source="stripe", reference="order_1",
            external_id="re_1", amount_minor=4999, currency="usd",
            subscription_id="sub_123", parent_external_id="ch_1",
        )
        env = fi.inspect()[0]
        self.assertEqual(env["subscription_id"], "sub_123")
        self.assertEqual(env["parent_external_id"], "ch_1")

    def test_subscription_and_parent_ids_absent_when_not_passed(self):
        fi = FinIntegrityClient(dry_run=True)
        fi.processor.record(
            type="payment", source="stripe", reference="order_1",
            external_id="ch_1", amount_minor=4999, currency="usd",
        )
        env = fi.inspect()[0]
        self.assertNotIn("subscription_id", env)
        self.assertNotIn("parent_external_id", env)

    def test_record_subscription_envelope(self):
        fi = FinIntegrityClient(dry_run=True)
        fi.processor.record_subscription(
            external_id="sub_123", amount_minor=2500, currency="USD",
            status="active", interval="month",
            current_period_start=datetime(2026, 7, 1, tzinfo=timezone.utc),
            current_period_end=datetime(2026, 8, 1, tzinfo=timezone.utc),
            trace_id="trace_sub",
        )
        env = fi.inspect()[0]
        self.assertEqual(env["event_type"], "subscription")
        self.assertEqual(env["side"], "processor")
        self.assertEqual(env["reference"], "sub_123")
        self.assertEqual(env["external_id"], "sub_123")
        self.assertEqual(env["amount"], {"minor": "2500", "currency": "usd"})
        self.assertEqual(env["status"], "active")
        self.assertEqual(env["interval"], "month")
        self.assertEqual(env["current_period_start"], "2026-07-01T00:00:00.000Z")
        self.assertEqual(env["current_period_end"], "2026-08-01T00:00:00.000Z")
        self.assertEqual(env["trace_id"], "trace_sub")

    def test_record_subscription_omits_optional_fields(self):
        fi = FinIntegrityClient(dry_run=True)
        fi.processor.record_subscription(
            external_id="sub_456", amount_minor=1000, currency="usd",
            status="trialing",
        )
        env = fi.inspect()[0]
        self.assertEqual(env["event_type"], "subscription")
        self.assertNotIn("interval", env)
        self.assertNotIn("current_period_start", env)
        self.assertNotIn("current_period_end", env)
        self.assertNotIn("trace_id", env)
        self.assertNotIn("metadata", env)

    # ---- idempotency basis: identity + observed state --------------------
    def _keys(self, fi):
        return [e["idempotency_key"] for e in fi.inspect()]

    def test_dispute_status_change_gets_a_new_key(self):
        # needs_response -> lost must reach the server; only `lost` is money out.
        fi = FinIntegrityClient(dry_run=True)
        for status in ("needs_response", "lost"):
            fi.processor.record(
                type="dispute", source="stripe", reference="order_1",
                external_id="dp_1", amount_minor=4999, currency="usd",
                status=status,
            )
        first, second = self._keys(fi)
        self.assertNotEqual(first, second)

    def test_same_fact_same_state_collapses_to_one_key(self):
        # Retry safety still holds: a crash/retry of the same fact dedupes.
        fi = FinIntegrityClient(dry_run=True)
        for _ in range(2):
            fi.processor.record(
                type="dispute", source="stripe", reference="order_1",
                external_id="dp_1", amount_minor=4999, currency="usd",
                status="needs_response",
            )
        first, second = self._keys(fi)
        self.assertEqual(first, second)

    def test_subscription_period_advance_gets_a_new_key(self):
        fi = FinIntegrityClient(dry_run=True)
        for end in (datetime(2026, 8, 1, tzinfo=timezone.utc),
                    datetime(2026, 9, 1, tzinfo=timezone.utc)):
            fi.processor.record_subscription(
                external_id="sub_acme_pro", amount_minor=2500, currency="usd",
                status="active", interval="month", current_period_end=end,
            )
        first, second = self._keys(fi)
        self.assertNotEqual(first, second)

    def test_subscription_status_change_gets_a_new_key(self):
        fi = FinIntegrityClient(dry_run=True)
        for status in ("active", "past_due"):
            fi.processor.record_subscription(
                external_id="sub_acme_pro", amount_minor=2500, currency="usd",
                status=status, interval="month",
                current_period_end=datetime(2026, 8, 1, tzinfo=timezone.utc),
            )
        first, second = self._keys(fi)
        self.assertNotEqual(first, second)

    def test_subscription_same_state_collapses_to_one_key(self):
        # occurred_at differs between the two calls but is not identity-or-state,
        # so it must not affect the key.
        fi = FinIntegrityClient(dry_run=True)
        for _ in range(2):
            fi.processor.record_subscription(
                external_id="sub_acme_pro", amount_minor=2500, currency="usd",
                status="active", interval="month",
                current_period_end=datetime(2026, 8, 1, tzinfo=timezone.utc),
            )
        first, second = self._keys(fi)
        self.assertEqual(first, second)

    def test_payout_status_change_gets_a_new_key(self):
        fi = FinIntegrityClient(dry_run=True)
        for status in ("pending", "paid"):
            fi.processor.record_payout(
                external_id="po_123", amount_minor=100000, currency="usd",
                status=status,
                arrival_at=datetime(2026, 8, 3, tzinfo=timezone.utc),
            )
        first, second = self._keys(fi)
        self.assertNotEqual(first, second)

    def test_payout_arrival_change_gets_a_new_key(self):
        fi = FinIntegrityClient(dry_run=True)
        for arrival in (datetime(2026, 8, 3, tzinfo=timezone.utc),
                        datetime(2026, 8, 5, tzinfo=timezone.utc)):
            fi.processor.record_payout(
                external_id="po_123", amount_minor=100000, currency="usd",
                status="pending", arrival_at=arrival,
            )
        first, second = self._keys(fi)
        self.assertNotEqual(first, second)

    def test_amount_change_gets_a_new_key(self):
        fi = FinIntegrityClient(dry_run=True)
        for minor in (4999, 5999):
            fi.processor.record(
                type="payment", source="stripe", reference="order_1",
                external_id="ch_1", amount_minor=minor, currency="usd",
            )
        first, second = self._keys(fi)
        self.assertNotEqual(first, second)

    def test_keys_match_node_sdk_byte_for_byte(self):
        # Golden values produced by the Node SDK (dist/index.js) for the same
        # facts. Guards cross-SDK dedup: if these drift, the same event sent from
        # a Node service and a Python service creates two rows instead of one.
        fi = FinIntegrityClient(dry_run=True)
        fi.processor.record_subscription(
            source="stripe", external_id="sub_acme_pro", amount_minor=2500,
            currency="usd", status="active", interval="month",
            current_period_end=datetime(2026, 8, 1, tzinfo=timezone.utc),
        )
        fi.processor.record(
            type="dispute", source="stripe", reference="order_1",
            external_id="dp_1", amount_minor=4999, currency="usd", status="lost",
        )
        sub, dispute = fi.inspect()
        self.assertEqual(sub["current_period_end"], "2026-08-01T00:00:00.000Z")
        self.assertEqual(sub["idempotency_key"], "fi_0d40fce657d7774ac47f5243626585f5310c44a4")
        self.assertEqual(dispute["idempotency_key"], "fi_2acd934e42c9c65987df0ad9a99e9850fbdc31a2")

    def test_iso_matches_js_millisecond_precision(self):
        fi = FinIntegrityClient(dry_run=True)
        fi.processor.record_payout(
            external_id="po_1", amount_minor=1, currency="usd",
            arrival_at=datetime(2026, 8, 3, 12, 30, 45, 123456, tzinfo=timezone.utc),
        )
        # JS Date.toISOString() truncates to 3 fractional digits; Python's
        # isoformat() would emit 6 here and none at zero microseconds.
        self.assertEqual(fi.inspect()[0]["arrival_at"], "2026-08-03T12:30:45.123Z")

    # ---- transport: a 200 is not proof every event stored -----------------
    def test_rejection_inside_a_200_surfaces_an_error(self):
        errors = []
        fi = _http_client(on_error=lambda e: errors.append(e))
        body = json.dumps({"results": [
            {"event_id": "fi_1", "status": "accepted"},
            {"event_id": "fi_2", "status": "rejected", "error": "amount.minor must be a string"},
        ]}).encode()
        fi.processor.record(
            type="payment", reference="o", external_id="ch_1",
            amount_minor=100, currency="usd",
        )
        with mock.patch("urllib.request.urlopen", return_value=_FakeResp(body)):
            fi.flush()  # must not raise into the caller
        fi.shutdown()
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], RejectedEventsError)
        self.assertEqual(errors[0].rejected, [
            {"event_id": "fi_2", "error": "amount.minor must be a string"},
        ])
        self.assertIn("rejected 1/1 event(s)", str(errors[0]))
        self.assertIn("amount.minor must be a string", str(errors[0]))

    def test_all_accepted_200_stays_quiet(self):
        errors = []
        fi = _http_client(on_error=lambda e: errors.append(e))
        body = json.dumps({"results": [{"event_id": "fi_1", "status": "accepted"}]}).encode()
        fi.processor.record(
            type="payment", reference="o", external_id="ch_1",
            amount_minor=100, currency="usd",
        )
        with mock.patch("urllib.request.urlopen", return_value=_FakeResp(body)):
            fi.flush()
        fi.shutdown()
        self.assertEqual(errors, [])

    def test_unparseable_200_body_stays_quiet(self):
        errors = []
        fi = _http_client(on_error=lambda e: errors.append(e))
        fi.processor.record(
            type="payment", reference="o", external_id="ch_1",
            amount_minor=100, currency="usd",
        )
        with mock.patch("urllib.request.urlopen", return_value=_FakeResp(b"not json")):
            fi.flush()
        fi.shutdown()
        self.assertEqual(errors, [])

    def test_rejection_is_not_retried(self):
        # A per-event reject is terminal — retrying would just re-reject.
        calls = []
        fi = _http_client(retries=3, on_error=lambda e: None)
        body = json.dumps({"results": [
            {"event_id": "fi_1", "status": "rejected", "error": "bad currency"},
        ]}).encode()

        def _fake(req, timeout=None):
            calls.append(req)
            return _FakeResp(body)

        fi.processor.record(
            type="payment", reference="o", external_id="ch_1",
            amount_minor=100, currency="usd",
        )
        with mock.patch("urllib.request.urlopen", side_effect=_fake):
            fi.flush()
        fi.shutdown()
        self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
