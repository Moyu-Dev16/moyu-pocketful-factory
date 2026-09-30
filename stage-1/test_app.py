"""Pocketful Stage 1 双录、幂等、并发与双时态合同测试。"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sys
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).parent))
import app  # noqa: E402


class WalletContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = app.Server(("127.0.0.1", 0), app.Handler)
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def request(self, method, path, body=None, token=None, key=None):
        """发送 JSON 请求并把成功与 HTTP 错误统一成 (状态码, 响应体)。"""
        data = None if body is None else json.dumps(body).encode()
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if key:
            headers["Idempotency-Key"] = key
        request = Request(self.base + path, data=data, headers=headers, method=method)
        try:
            with urlopen(request, timeout=5) as response:
                raw = response.read()
                return response.status, json.loads(raw) if raw else None
        except HTTPError as error:
            raw = error.read()
            return error.code, json.loads(raw) if raw else None

    def setUp(self):
        fixture = {
            "currency": "EUR", "minor_units": 2,
            "settlement_operator_ids": ["u_alice"],
            "users": [
                {"id": "u_alice", "email": "alice@example.com", "password": "password1",
                 "handle": "alice", "display_name": "Alice", "balance": 100},
                {"id": "u_bob", "email": "bob@example.com", "password": "password2",
                 "handle": "bob", "display_name": "Bob", "balance": 0},
            ],
        }
        self.assertEqual(204, self.request("POST", "/_test/reset", fixture)[0])
        self.alice = self.request("POST", "/auth/login", {
            "email": "alice@example.com", "password": "password1"
        })[1]["token"]
        self.bob = self.request("POST", "/auth/login", {
            "email": "bob@example.com", "password": "password2"
        })[1]["token"]

    def pay(self, amount, key, **extra):
        return self.request("POST", "/payments", {
            "to_handle": "bob", "amount": amount, **extra
        }, self.alice, key)

    def export_document(self):
        """读取可直接回灌 import 的完整导出文档。"""
        status, document = self.request("GET", "/_test/export")
        self.assertEqual(200, status)
        return document

    def test_double_entry_conservation_and_replay(self):
        status, payment = self.pay(35, "double-entry")
        self.assertEqual(201, status)
        _, exported = self.request("GET", "/_test/export")
        state = exported["state"]
        entries = [e for e in state["ledger"] if e["payment_id"] == payment["payment_id"]]
        self.assertEqual(2, len(entries))
        self.assertEqual({"debit", "credit"}, {e["side"] for e in entries})
        self.assertEqual(0, sum(e["delta"] for e in entries))
        replay = {uid: user["opening_balance"] for uid, user in state["users"].items()}
        for entry in state["ledger"]:
            replay[entry["account_id"]] += entry["delta"]
        self.assertEqual({uid: u["balance"] for uid, u in state["users"].items()}, replay)
        self.assertEqual(100, sum(replay.values()))

    def test_concurrent_overspend_is_linearized(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda key: self.pay(80, key)[0], ("over-1", "over-2")))
        self.assertEqual([201, 409], sorted(results))
        self.assertEqual(20, self.request("GET", "/me", token=self.alice)[1]["balance"])

    def test_concurrent_same_key_replays_original_status_and_response(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: self.pay(10, "same-key"), range(8)))
        self.assertTrue(all(status == 201 for status, _ in results))
        self.assertEqual(1, len({body["payment_id"] for _, body in results}))
        self.assertEqual(90, self.request("GET", "/me", token=self.alice)[1]["balance"])

    def test_same_key_different_payload_conflicts(self):
        self.assertEqual(201, self.pay(10, "conflict")[0])
        self.assertEqual(409, self.pay(11, "conflict")[0])

    def test_failure_does_not_poison_retry(self):
        self.assertEqual(409, self.pay(101, "retry-after-failure")[0])
        self.assertEqual(201, self.pay(100, "retry-after-failure")[0])

    def test_all_declared_write_paths_require_idempotency_key(self):
        paths = [
            ("/payments", {"to_handle": "bob", "amount": 1}),
            ("/requests", {"payer_handle": "bob", "amount": 1}),
            ("/splits", {"amount": 1, "participant_handles": ["bob"]}),
            ("/settlements", {"transfers": [{"from_handle": "alice", "to_handle": "bob", "amount": 1}]}),
            ("/requests/missing/pay", {}),
        ]
        for path, body in paths:
            with self.subTest(path=path):
                status, response = self.request("POST", path, body, self.alice)
                self.assertEqual(400, status)
                self.assertEqual("missing_idempotency_key", response["error"]["code"])

    def test_request_payment_and_settlement_are_double_entry(self):
        status, request = self.request(
            "POST", "/requests", {"payer_handle": "alice", "amount": 15},
            self.bob, "create-request"
        )
        self.assertEqual(201, status)
        self.assertEqual(201, self.request(
            "POST", f"/requests/{request['request_id']}/pay", {}, self.alice, "pay-request"
        )[0])
        self.assertEqual(201, self.request(
            "POST", "/settlements", {
                "transfers": [{"from_handle": "alice", "to_handle": "bob", "amount": 5}]
            }, self.alice, "settle"
        )[0])
        _, exported = self.request("GET", "/_test/export")
        grouped = {}
        for entry in exported["state"]["ledger"]:
            grouped.setdefault(entry["transaction_id"], []).append(entry)
        self.assertEqual(2, len(grouped))
        for entries in grouped.values():
            self.assertEqual(2, len(entries))
            self.assertEqual(0, sum(entry["delta"] for entry in entries))
        self.assertEqual(100, sum(u["balance"] for u in exported["state"]["users"].values()))

    def test_bitemporal_correction_preserves_old_known_snapshot(self):
        effective = "2026-09-01T12:00:00+00:00"
        _, payment = self.pay(30, "historic-payment", effective_at=effective)
        before_correction = payment["recorded_at"]
        status, correction = self.request(
            "POST", f"/payments/{payment['payment_id']}/corrections",
            {"expected_revision": 1, "amount": 20, "effective_at": effective, "reason": "金额更正"},
            self.alice, "correction-key"
        )
        self.assertEqual(201, status)
        old = self.request(
            "GET", f"/statement?effective_at=2026-09-02T00:00:00%2B00:00&recorded_at={before_correction.replace('+', '%2B')}",
            token=self.alice
        )[1]
        current = self.request(
            "GET", f"/statement?effective_at=2026-09-02T00:00:00%2B00:00&recorded_at={correction['recorded_at'].replace('+', '%2B')}",
            token=self.alice
        )[1]
        self.assertEqual(70, old["balance"])
        self.assertEqual(80, current["balance"])
        _, exported = self.request("GET", "/_test/export")
        revisions = exported["state"]["payments"][0]["revisions"]
        self.assertEqual([30, 20], [revision["amount"] for revision in revisions])

    def test_failed_import_is_atomic_and_preserves_every_exported_field(self):
        self.assertEqual(201, self.pay(10, "state-before-bad-import")[0])
        before = self.export_document()
        malformed = json.loads(json.dumps(before))
        malformed["state"]["users"]["u_alice"]["balance"] = 999
        self.assertEqual(422, self.request("POST", "/_test/import", malformed)[0])
        self.assertEqual(before, self.export_document())

    def test_import_rejects_single_sided_transaction_without_state_change(self):
        _, payment = self.pay(10, "paired-before-corruption")
        before = self.export_document()
        malformed = json.loads(json.dumps(before))
        payment_id = payment["payment_id"]
        malformed["state"]["ledger"] = [
            entry for entry in malformed["state"]["ledger"]
            if not (entry["payment_id"] == payment_id and entry["side"] == "credit")
        ]
        self.assertEqual(422, self.request("POST", "/_test/import", malformed)[0])
        self.assertEqual(before, self.export_document())

    def test_failed_reset_is_atomic_and_preserves_every_exported_field(self):
        self.assertEqual(201, self.pay(5, "state-before-bad-reset")[0])
        before = self.export_document()
        invalid_fixture = {
            "currency": "EUR", "minor_units": 2,
            "users": [
                {"id": "u_alice", "email": "alice@example.com", "password": "password1",
                 "handle": "alice", "balance": 0},
                {"id": "u_bob", "email": "bob@example.com", "password": "password2",
                 "handle": "bob", "balance": 0},
            ],
            "payments": [{
                "id": "p_invalid", "from_user_id": "u_alice", "to_user_id": "u_bob",
                "amount": 10, "created_at": "2026-09-01T00:00:00+00:00"
            }],
        }
        self.assertEqual(422, self.request("POST", "/_test/reset", invalid_fixture)[0])
        self.assertEqual(before, self.export_document())

    def test_valid_export_import_round_trip(self):
        self.assertEqual(201, self.pay(25, "round-trip-payment")[0])
        before = self.export_document()
        self.assertEqual(204, self.request("POST", "/_test/import", before)[0])
        self.assertEqual(before, self.export_document())

    def test_import_rejects_split_effective_time_without_state_change(self):
        _, payment = self.pay(10, "time-pair-payment")
        before = self.export_document()
        malformed = json.loads(json.dumps(before))
        for entry in malformed["state"]["ledger"]:
            if entry["payment_id"] == payment["payment_id"] and entry["side"] == "credit":
                entry["effective_at"] = "2099-01-01T00:00:00+00:00"
        self.assertEqual(422, self.request("POST", "/_test/import", malformed)[0])
        self.assertEqual(before, self.export_document())

    def test_import_rejects_cross_payment_entry_binding_without_state_change(self):
        _, first = self.pay(10, "first-binding-payment")
        _, second = self.pay(5, "second-binding-payment")
        before = self.export_document()
        malformed = json.loads(json.dumps(before))
        for entry in malformed["state"]["ledger"]:
            if entry["payment_id"] == first["payment_id"] and entry["side"] == "credit":
                entry["payment_id"] = second["payment_id"]
        self.assertEqual(422, self.request("POST", "/_test/import", malformed)[0])
        self.assertEqual(before, self.export_document())

    def test_corrected_payment_export_import_round_trip(self):
        effective = "2026-09-01T12:00:00+00:00"
        _, payment = self.pay(30, "correction-round-trip", effective_at=effective)
        status, _ = self.request(
            "POST", f"/payments/{payment['payment_id']}/corrections",
            {"expected_revision": 1, "amount": 20, "effective_at": effective, "reason": "回灌校验"},
            self.alice, "correction-round-trip-key"
        )
        self.assertEqual(201, status)
        before = self.export_document()
        self.assertEqual(204, self.request("POST", "/_test/import", before)[0])
        self.assertEqual(before, self.export_document())


if __name__ == "__main__":
    unittest.main()
