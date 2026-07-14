import unittest

from fin_integrity import FinIntegrityClient


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


if __name__ == "__main__":
    unittest.main()
