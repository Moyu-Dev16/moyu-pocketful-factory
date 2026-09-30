"""Pocketful Stage 1: payments, requests, splits, settlements, and activity feed.

Full clean-room implementation conforming to Pocketful Stage 1 specification.
"""
from __future__ import annotations

import copy
from datetime import datetime, timezone, timedelta
import hashlib
import json
import os
import re
import threading
from urllib.parse import parse_qs, urlparse
import uuid

try:
    from .passwords import hash_password, verify_password
except ImportError:
    from passwords import hash_password, verify_password

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Global in-memory state protected by a re-entrant lock
STATE_LOCK = threading.RLock()

STATE: dict = {
    "currency": "EUR",
    "minor_units": 2,
    "settlement_operator_ids": set(),
    "users": {},            # user_id -> user dict
    "by_handle": {},        # handle -> user_id
    "by_email": {},         # email.lower() -> user_id
    "tokens": {},           # token -> user_id
    "payments": [],         # list of payment dicts
    "requests": [],         # list of request dicts
    "ledger": [],           # append-only double-entry journal
    "initial_total": 0,     # reset fixture conservation anchor
    "idempotency": {},      # (user_id, method, path, key) -> {"canonical_body": ..., "response": ..., "status": ...}
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_rfc3339(value: str | None) -> datetime | None:
    """解析带时区的 RFC3339 时间，拒绝含糊的本地时间。"""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed


def ledger_balances(effective_at: datetime | None = None,
                    recorded_at: datetime | None = None) -> dict[str, int]:
    """只从期初余额与不可变分录重放余额，可按双时态截断。调用方须持有锁。"""
    balances = {
        uid: int(user.get("opening_balance", 0))
        for uid, user in STATE["users"].items()
    }
    for entry in STATE["ledger"]:
        effective = parse_rfc3339(entry["effective_at"])
        recorded = parse_rfc3339(entry["recorded_at"])
        if effective_at is not None and (effective is None or effective > effective_at):
            continue
        if recorded_at is not None and (recorded is None or recorded > recorded_at):
            continue
        balances[entry["account_id"]] = balances.get(entry["account_id"], 0) + entry["delta"]
    return balances


def build_transfer_entries(transaction_id: str, from_user_id: str, to_user_id: str,
                           amount: int, effective_at: str, recorded_at: str,
                           *, payment_id: str | None = None,
                           entry_kind: str = "payment") -> list[dict]:
    """构造恰好一借一贷的不可变分录对；借贷金额和币种始终一致。"""
    common = {
        "transaction_id": transaction_id,
        "payment_id": payment_id,
        "amount": amount,
        "currency": STATE["currency"],
        "effective_at": effective_at,
        "recorded_at": recorded_at,
        "kind": entry_kind,
    }
    return [
        {**common, "entry_id": f"le_{uuid.uuid4().hex}", "account_id": from_user_id,
         "side": "debit", "delta": -amount},
        {**common, "entry_id": f"le_{uuid.uuid4().hex}", "account_id": to_user_id,
         "side": "credit", "delta": amount},
    ]


def commit_transfers(transfers: list[dict]) -> None:
    """在线性化临界区内校验并一次提交多笔转账，同时核对缓存与账本重放。"""
    proposed = {uid: user["balance"] for uid, user in STATE["users"].items()}
    pending_entries = []
    for transfer in transfers:
        amount = transfer["amount"]
        proposed[transfer["from_user_id"]] -= amount
        proposed[transfer["to_user_id"]] += amount
        pending_entries.extend(build_transfer_entries(
            transfer["transaction_id"], transfer["from_user_id"], transfer["to_user_id"],
            amount, transfer["effective_at"], transfer["recorded_at"],
            payment_id=transfer.get("payment_id"), entry_kind=transfer.get("kind", "payment")
        ))

    if any(balance < 0 for balance in proposed.values()):
        raise ValueError("insufficient_funds")
    if sum(proposed.values()) != STATE["initial_total"]:
        raise RuntimeError("balance_conservation_violation")
    if any(sum(e["delta"] for e in pending_entries if e["transaction_id"] == tx) != 0
           for tx in {e["transaction_id"] for e in pending_entries}):
        raise RuntimeError("unbalanced_transaction")

    # 以当前已知账本重放每个业务时间边界，禁止回填或更正制造历史透支。
    historical = {
        uid: int(user.get("opening_balance", 0))
        for uid, user in STATE["users"].items()
    }
    if any(balance < 0 for balance in historical.values()):
        raise ValueError("historical_overdraft")
    grouped: dict[datetime, list[dict]] = {}
    for entry in [*STATE["ledger"], *pending_entries]:
        effective = parse_rfc3339(entry["effective_at"])
        if effective is None:
            raise RuntimeError("invalid_ledger_time")
        grouped.setdefault(effective, []).append(entry)
    for effective in sorted(grouped):
        for entry in grouped[effective]:
            historical[entry["account_id"]] += entry["delta"]
        if any(balance < 0 for balance in historical.values()):
            raise ValueError("historical_overdraft")

    expected = ledger_balances()
    for entry in pending_entries:
        expected[entry["account_id"]] += entry["delta"]
    if expected != proposed:
        raise RuntimeError("ledger_cache_mismatch")

    for uid, balance in proposed.items():
        STATE["users"][uid]["balance"] = balance
    STATE["ledger"].extend(pending_entries)


def is_valid_amount(val) -> bool:
    if isinstance(val, bool):
        return False
    if not isinstance(val, (int, float)):
        return False
    if isinstance(val, float) and not val.is_integer():
        return False
    ival = int(val)
    return 1 <= ival <= 1_000_000_000


def normalize_json_value(val):
    if isinstance(val, bool):
        return val
    if isinstance(val, float) and val.is_integer():
        return int(val)
    if isinstance(val, dict):
        return {k: normalize_json_value(v) for k, v in val.items()}
    if isinstance(val, list):
        return [normalize_json_value(v) for v in val]
    return val


def canonical_json(val) -> str:
    return json.dumps(normalize_json_value(val), sort_keys=True, separators=(',', ':'))


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def send_json(self, status: int, payload) -> None:
        body = b"" if payload is None else json.dumps(payload).encode("utf-8")
        self.send_response(status)
        if body:
            self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def fail(self, status: int, code: str) -> None:
        self.send_json(status, {"error": {"code": code, "message": code}})

    def read_body(self):
        length_header = self.headers.get("Content-Length")
        if length_header is None:
            return {}
        try:
            length = int(length_header)
        except ValueError:
            return None
        raw = self.rfile.read(length) if length else b""
        if not raw:
            return {}
        try:
            parsed = json.loads(raw.decode("utf-8"))
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None

    def get_auth_user(self):
        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return None
        token = auth[7:].strip()
        user_id = STATE["tokens"].get(token)
        if not user_id:
            return None
        return STATE["users"].get(user_id)

    def do_GET(self):
        self.route("GET")

    def do_POST(self):
        self.route("POST")

    def route(self, method: str) -> None:
        parsed_url = urlparse(self.path)
        path = parsed_url.path.rstrip("/") or "/"

        # Unauthenticated endpoints
        if method == "GET" and path == "/health":
            return self.send_json(200, {"status": "ok"})

        if method == "POST" and path == "/_test/reset":
            return self.handle_reset()

        if method == "GET" and path == "/_test/export":
            return self.handle_export()

        if method == "POST" and path == "/_test/import":
            return self.handle_import()

        if method == "POST" and path == "/auth/signup":
            return self.handle_signup()

        if method == "POST" and path == "/auth/login":
            return self.handle_login()

        # All other endpoints require authentication
        user = self.get_auth_user()
        if not user:
            return self.fail(401, "unauthenticated")

        # GET endpoints
        if method == "GET":
            if path == "/me":
                return self.handle_me(user)
            if path == "/activity":
                return self.handle_activity(user, parsed_url.query)
            if path == "/requests":
                return self.handle_requests_list(user, parsed_url.query)
            if path == "/statement":
                return self.handle_statement(user, parsed_url.query)
            if path == "/ledger":
                return self.handle_ledger(user, parsed_url.query)
            return self.fail(404, "not_found")

        # POST endpoints
        if method == "POST":
            # Idempotent write paths
            idempotent_paths = (
                "/payments",
                "/requests",
                "/splits",
                "/settlements",
            )
            is_pay_request = re.fullmatch(r"/requests/[^/]+/pay", path)
            is_correction = re.fullmatch(r"/payments/[^/]+/corrections", path)

            if path in idempotent_paths or is_pay_request or is_correction:
                return self.handle_idempotent_post(method, path, user)

            # Non-idempotent write paths
            m_decline = re.fullmatch(r"/requests/([^/]+)/decline", path)
            if m_decline:
                return self.handle_request_decline(m_decline.group(1), user)

            m_cancel = re.fullmatch(r"/requests/([^/]+)/cancel", path)
            if m_cancel:
                return self.handle_request_cancel(m_cancel.group(1), user)

            return self.fail(404, "not_found")

        return self.fail(501, "not_implemented")

    # =========================================================================
    # Test & Setup Endpoints
    # =========================================================================

    def handle_reset(self):
        fixture = self.read_body()
        if fixture is None or not isinstance(fixture, dict):
            return self.fail(400, "malformed_request")

        # Check negative balance in fixture
        for u in fixture.get("users", []):
            if u.get("balance", 0) < 0:
                return self.fail(422, "validation_failed")

        with STATE_LOCK:
            STATE["currency"] = fixture.get("currency", "EUR")
            STATE["minor_units"] = fixture.get("minor_units", 2)
            STATE["settlement_operator_ids"] = set(fixture.get("settlement_operator_ids", []))
            STATE["users"] = {}
            STATE["by_handle"] = {}
            STATE["by_email"] = {}
            STATE["tokens"] = {}
            STATE["payments"] = []
            STATE["requests"] = []
            STATE["ledger"] = []
            STATE["idempotency"] = {}

            for u in fixture.get("users", []):
                uid = u["id"]
                raw_pwd = u.get("password", "")
                if raw_pwd.startswith("scrypt$"):
                    hashed_pwd = raw_pwd
                else:
                    deterministic_salt = hashlib.md5(f"salt:{uid}:{raw_pwd}".encode()).hexdigest()[:32]
                    hashed_pwd = hash_password(raw_pwd, salt=deterministic_salt)
                user_obj = {
                    "id": uid,
                    "email": u["email"],
                    "password": hashed_pwd,
                    "display_name": u.get("display_name", u.get("handle", "")),
                    "handle": u["handle"],
                    "balance": int(u.get("balance", 0)),
                    "opening_balance": int(u.get("balance", 0))
                }
                STATE["users"][uid] = user_obj
                STATE["by_handle"][user_obj["handle"]] = uid
                STATE["by_email"][user_obj["email"].lower()] = uid

            for p in fixture.get("payments", []):
                from_u = STATE["users"].get(p["from_user_id"])
                to_u = STATE["users"].get(p["to_user_id"])
                pm = {
                    "payment_id": p.get("id") or f"p_{uuid.uuid4().hex[:8]}",
                    "from_user_id": p["from_user_id"],
                    "from_handle": from_u["handle"] if from_u else "",
                    "to_user_id": p["to_user_id"],
                    "to_handle": to_u["handle"] if to_u else "",
                    "amount": int(p["amount"]),
                    "currency": STATE["currency"],
                    "note": p.get("note", ""),
                    "visibility": p.get("visibility", "public"),
                    "request_id": p.get("request_id"),
                    "settlement_id": p.get("settlement_id"),
                    "created_at": p.get("created_at") or "2026-09-01T00:00:00+00:00"
                }
                pm["effective_at"] = p.get("effective_at", pm["created_at"])
                pm["recorded_at"] = p.get("recorded_at", pm["created_at"])
                pm["revisions"] = copy.deepcopy(p.get("revisions", [{
                    "revision": 1, "amount": pm["amount"],
                    "effective_at": pm["effective_at"], "recorded_at": pm["recorded_at"],
                    "reason": ""
                }]))
                STATE["payments"].append(pm)

                # fixture 中 payment 是既有历史，反推出可重放的期初余额并补齐双录分录。
                created_at = pm["effective_at"]
                STATE["users"][pm["from_user_id"]]["opening_balance"] += pm["amount"]
                STATE["users"][pm["to_user_id"]]["opening_balance"] -= pm["amount"]
                STATE["ledger"].extend(build_transfer_entries(
                    f"tx_seed_{pm['payment_id']}", pm["from_user_id"], pm["to_user_id"],
                    pm["amount"], created_at, pm["recorded_at"], payment_id=pm["payment_id"],
                    entry_kind="fixture"
                ))

            for r in fixture.get("requests", []):
                req_u = STATE["users"].get(r["requester_id"])
                payer_u = STATE["users"].get(r["payer_id"])
                rq = {
                    "request_id": r.get("id") or f"rq_{uuid.uuid4().hex[:8]}",
                    "requester_id": r["requester_id"],
                    "requester_handle": req_u["handle"] if req_u else "",
                    "payer_id": r["payer_id"],
                    "payer_handle": payer_u["handle"] if payer_u else "",
                    "amount": int(r["amount"]),
                    "currency": STATE["currency"],
                    "note": r.get("note", ""),
                    "status": r.get("status", "pending"),
                    "payment_id": r.get("payment_id"),
                    "created_at": r.get("created_at") or "2026-09-01T00:00:00+00:00"
                }
                STATE["requests"].append(rq)

            STATE["initial_total"] = sum(u["balance"] for u in STATE["users"].values())
            if ledger_balances() != {uid: u["balance"] for uid, u in STATE["users"].items()}:
                return self.fail(422, "validation_failed")
            try:
                commit_transfers([])
            except (ValueError, RuntimeError):
                return self.fail(422, "validation_failed")

        return self.send_json(204, None)

    def handle_export(self):
        with STATE_LOCK:
            # Serialize idempotency keys as strings
            idem_export = {}
            for k, v in STATE["idempotency"].items():
                # k is (user_id, method, path, key)
                key_str = json.dumps(list(k))
                idem_export[key_str] = v

            state_snapshot = {
                "currency": STATE["currency"],
                "minor_units": STATE["minor_units"],
                "settlement_operator_ids": list(STATE["settlement_operator_ids"]),
                "users": copy.deepcopy(STATE["users"]),
                "by_handle": copy.deepcopy(STATE["by_handle"]),
                "by_email": copy.deepcopy(STATE["by_email"]),
                "tokens": copy.deepcopy(STATE["tokens"]),
                "payments": copy.deepcopy(STATE["payments"]),
                "requests": copy.deepcopy(STATE["requests"]),
                "ledger": copy.deepcopy(STATE["ledger"]),
                "initial_total": STATE["initial_total"],
                "idempotency": idem_export,
            }

        return self.send_json(200, {
            "track": "pocketful",
            "format_version": 1,
            "state": state_snapshot
        })

    def handle_import(self):
        body = self.read_body()
        if body is None or not isinstance(body, dict):
            return self.fail(400, "malformed_request")

        if (body.get("track") != "pocketful" or
                body.get("format_version") != 1 or
                "state" not in body or
                not isinstance(body["state"], dict)):
            return self.fail(422, "validation_failed")

        s = body["state"]
        required_keys = ("currency", "minor_units", "users", "by_handle", "by_email", "tokens", "payments", "requests")
        if not all(k in s for k in required_keys):
            return self.fail(422, "validation_failed")

        with STATE_LOCK:
            STATE["currency"] = s["currency"]
            STATE["minor_units"] = s["minor_units"]
            STATE["settlement_operator_ids"] = set(s.get("settlement_operator_ids", []))
            STATE["users"] = copy.deepcopy(s["users"])
            STATE["by_handle"] = copy.deepcopy(s["by_handle"])
            STATE["by_email"] = copy.deepcopy(s["by_email"])
            STATE["tokens"] = copy.deepcopy(s["tokens"])
            STATE["payments"] = copy.deepcopy(s["payments"])
            STATE["requests"] = copy.deepcopy(s["requests"])
            STATE["ledger"] = copy.deepcopy(s.get("ledger", []))
            STATE["initial_total"] = int(s.get(
                "initial_total", sum(u["balance"] for u in STATE["users"].values())
            ))

            for user in STATE["users"].values():
                user.setdefault("opening_balance", user["balance"])

            cached = {uid: u["balance"] for uid, u in STATE["users"].items()}
            if STATE["ledger"] and ledger_balances() != cached:
                return self.fail(422, "validation_failed")
            if sum(cached.values()) != STATE["initial_total"]:
                return self.fail(422, "validation_failed")

            STATE["idempotency"] = {}
            for k_str, v in s.get("idempotency", {}).items():
                try:
                    k_tuple = tuple(json.loads(k_str))
                    STATE["idempotency"][k_tuple] = v
                except Exception:
                    pass

        return self.send_json(204, None)

    # =========================================================================
    # Auth Endpoints
    # =========================================================================

    def handle_signup(self):
        data = self.read_body()
        if data is None or not isinstance(data, dict):
            return self.fail(400, "malformed_request")

        email = data.get("email")
        password = data.get("password")
        display_name = data.get("display_name")

        if not isinstance(email, str) or not isinstance(password, str):
            return self.fail(400, "malformed_request")

        if not re.fullmatch(r"[^@\s]+@[^@\s]+", email) or len(password) < 8:
            return self.fail(422, "validation_failed")

        local_part = email.split("@")[0].lower()
        derived_handle = re.sub(r"[^a-z0-9_]", "_", local_part)[:20]
        if not re.fullmatch(r"^[a-z0-9_]{1,20}$", derived_handle):
            return self.fail(422, "validation_failed")

        with STATE_LOCK:
            if email.lower() in STATE["by_email"]:
                return self.fail(409, "email_taken")

            if derived_handle in STATE["by_handle"]:
                return self.fail(409, "handle_taken")

            user_id = f"u_{uuid.uuid4().hex[:8]}"
            user = {
                "id": user_id,
                "email": email,
                "password": hash_password(password),
                "display_name": display_name or derived_handle,
                "handle": derived_handle,
                "balance": 0,
                "opening_balance": 0
            }
            STATE["users"][user_id] = user
            STATE["by_email"][email.lower()] = user_id
            STATE["by_handle"][derived_handle] = user_id

            token = uuid.uuid4().hex
            STATE["tokens"][token] = user_id

        return self.send_json(201, {
            "user_id": user_id,
            "display_name": user["display_name"],
            "token": token
        })

    def handle_login(self):
        data = self.read_body()
        if data is None or not isinstance(data, dict):
            return self.fail(400, "malformed_request")

        email = data.get("email")
        password = data.get("password")
        if not isinstance(email, str) or not isinstance(password, str):
            return self.fail(400, "malformed_request")

        with STATE_LOCK:
            user_id = STATE["by_email"].get(email.lower())
            if not user_id:
                return self.fail(401, "unauthenticated")
            user = STATE["users"].get(user_id)
            if not user or not verify_password(password, user["password"]):
                return self.fail(401, "unauthenticated")

            token = uuid.uuid4().hex
            STATE["tokens"][token] = user_id

        return self.send_json(200, {
            "user_id": user["id"],
            "display_name": user["display_name"],
            "token": token
        })

    # =========================================================================
    # Account & Activity
    # =========================================================================

    def handle_me(self, user):
        with STATE_LOCK:
            current_user = STATE["users"].get(user["id"])
            return self.send_json(200, {
                "user_id": current_user["id"],
                "display_name": current_user["display_name"],
                "handle": current_user["handle"],
                "balance": current_user["balance"],
                "currency": STATE["currency"],
                "minor_units": STATE["minor_units"]
            })

    def handle_activity(self, user, query_str: str):
        query = parse_qs(query_str, keep_blank_values=True)
        limit = 50
        offset = 0

        if "limit" in query:
            raw_limit = query["limit"][-1]
            if not re.fullmatch(r"^[0-9]+$", raw_limit):
                return self.fail(422, "validation_failed")
            limit = int(raw_limit)
            if not 1 <= limit <= 200:
                return self.fail(422, "validation_failed")

        if "offset" in query:
            raw_offset = query["offset"][-1]
            if not re.fullmatch(r"^[0-9]+$", raw_offset):
                return self.fail(422, "validation_failed")
            offset = int(raw_offset)
            if offset < 0:
                return self.fail(422, "validation_failed")

        with STATE_LOCK:
            caller_id = user["id"]
            matched = [
                p for p in reversed(STATE["payments"])
                if p["visibility"] == "public" or p["from_user_id"] == caller_id or p["to_user_id"] == caller_id
            ]

            total = len(matched)
            page = matched[offset: offset + limit]
            has_more = (offset + limit < total)

        return self.send_json(200, {
            "payments": page,
            "has_more": has_more
        })

    def handle_requests_list(self, user, query_str: str):
        query = parse_qs(query_str, keep_blank_values=True)
        direction = None
        status = None
        limit = 50
        offset = 0

        if "direction" in query:
            direction = query["direction"][-1]
            if direction not in ("incoming", "outgoing"):
                return self.fail(422, "validation_failed")

        if "status" in query:
            status = query["status"][-1]
            if status not in ("pending", "paid", "declined", "cancelled"):
                return self.fail(422, "validation_failed")

        if "limit" in query:
            raw_limit = query["limit"][-1]
            if not re.fullmatch(r"^[0-9]+$", raw_limit):
                return self.fail(422, "validation_failed")
            limit = int(raw_limit)
            if not 1 <= limit <= 200:
                return self.fail(422, "validation_failed")

        if "offset" in query:
            raw_offset = query["offset"][-1]
            if not re.fullmatch(r"^[0-9]+$", raw_offset):
                return self.fail(422, "validation_failed")
            offset = int(raw_offset)
            if offset < 0:
                return self.fail(422, "validation_failed")

        with STATE_LOCK:
            caller_id = user["id"]
            filtered = []
            for r in reversed(STATE["requests"]):
                is_requester = (r["requester_id"] == caller_id)
                is_payer = (r["payer_id"] == caller_id)

                if not (is_requester or is_payer):
                    continue

                if direction == "incoming" and not is_payer:
                    continue
                if direction == "outgoing" and not is_requester:
                    continue

                if status and r["status"] != status:
                    continue

                filtered.append(r)

            total = len(filtered)
            page = filtered[offset: offset + limit]
            has_more = (offset + limit < total)

        return self.send_json(200, {
            "requests": page,
            "has_more": has_more
        })

    def _snapshot_times(self, query_str: str):
        """统一解析双时态查询参数；effective_at 是业务时间，recorded_at 是认知时间。"""
        query = parse_qs(query_str, keep_blank_values=True)
        now = datetime.now(timezone.utc)
        effective_raw = (query.get("effective_at") or query.get("as_of") or [None])[-1]
        recorded_raw = (query.get("recorded_at") or query.get("known_at") or [None])[-1]
        effective = parse_rfc3339(effective_raw) if effective_raw is not None else now
        recorded = parse_rfc3339(recorded_raw) if recorded_raw is not None else now
        if effective is None or recorded is None:
            return None
        return effective, recorded

    def handle_statement(self, user, query_str: str):
        """返回调用方在指定有效时间与记录时间下可复现的余额快照和分录。"""
        times = self._snapshot_times(query_str)
        if times is None:
            return self.fail(422, "validation_failed")
        effective, recorded = times
        with STATE_LOCK:
            entries = []
            for entry in STATE["ledger"]:
                if entry["account_id"] != user["id"]:
                    continue
                eff = parse_rfc3339(entry["effective_at"])
                rec = parse_rfc3339(entry["recorded_at"])
                if eff is not None and rec is not None and eff <= effective and rec <= recorded:
                    entries.append(copy.deepcopy(entry))
            entries.sort(key=lambda item: (item["effective_at"], item["recorded_at"], item["entry_id"]))
            balances = ledger_balances(effective, recorded)
            return self.send_json(200, {
                "user_id": user["id"],
                "balance": balances[user["id"]],
                "currency": STATE["currency"],
                "effective_at": effective.isoformat(),
                "recorded_at": recorded.isoformat(),
                "entries": entries,
            })

    def handle_ledger(self, user, query_str: str):
        """公开当前用户可见的不可变账本，用于审计一借一贷和重放一致性。"""
        times = self._snapshot_times(query_str)
        if times is None:
            return self.fail(422, "validation_failed")
        effective, recorded = times
        with STATE_LOCK:
            visible_tx = {
                entry["transaction_id"] for entry in STATE["ledger"]
                if entry["account_id"] == user["id"]
            }
            entries = [
                copy.deepcopy(entry) for entry in STATE["ledger"]
                if entry["transaction_id"] in visible_tx
                and parse_rfc3339(entry["effective_at"]) <= effective
                and parse_rfc3339(entry["recorded_at"]) <= recorded
            ]
            return self.send_json(200, {"entries": entries})

    # =========================================================================
    # Idempotent Write Handler
    # =========================================================================

    def handle_idempotent_post(self, method: str, path: str, user):
        body = self.read_body()
        if body is None or not isinstance(body, dict):
            return self.fail(400, "malformed_request")

        # Check Idempotency-Key
        idem_key = self.headers.get("Idempotency-Key")
        if idem_key is None or len(idem_key) == 0:
            return self.fail(400, "missing_idempotency_key")
        if len(idem_key) > 255:
            return self.fail(422, "validation_failed")

        canon_body = canonical_json(body)
        idem_token = (user["id"], method, path, idem_key)

        with STATE_LOCK:
            # Check previously completed idempotent request
            if idem_token in STATE["idempotency"]:
                record = STATE["idempotency"][idem_token]
                if record["canonical_body"] == canon_body:
                    return self.send_json(record["status"], record["response"])
                else:
                    return self.fail(409, "idempotency_key_reuse")

            # Route to respective handler
            if path == "/payments":
                status, resp = self.exec_payment(user, body)
            elif path == "/requests":
                status, resp = self.exec_request(user, body)
            elif path == "/splits":
                status, resp = self.exec_split(user, body)
            elif path == "/settlements":
                status, resp = self.exec_settlement(user, body)
            elif path.startswith("/requests/") and path.endswith("/pay"):
                request_id = path.split("/")[2]
                status, resp = self.exec_request_pay(request_id, user, body)
            elif path.startswith("/payments/") and path.endswith("/corrections"):
                payment_id = path.split("/")[2]
                status, resp = self.exec_payment_correction(payment_id, user, body)
            else:
                return self.fail(404, "not_found")

            # If successful (201), register idempotency key
            if status == 201:
                STATE["idempotency"][idem_token] = {
                    "canonical_body": canon_body,
                    "response": resp,
                    "status": 201
                }
                return self.send_json(201, resp)
            else:
                return self.fail(status, resp)

    # =========================================================================
    # Business Logic Execution (Under STATE_LOCK)
    # =========================================================================

    def exec_payment(self, user, body) -> tuple[int, any]:
        if "to_handle" not in body or "amount" not in body:
            return 422, "validation_failed"

        to_handle = body.get("to_handle")
        amount = body.get("amount")
        note = body.get("note", "")
        visibility = body.get("visibility", "public")
        effective_at = body.get("effective_at")

        if not isinstance(to_handle, str):
            return 422, "validation_failed"
        if not re.fullmatch(r"^[a-z0-9_]{1,20}$", to_handle):
            return 404, "not_found"

        if to_handle == user["handle"]:
            return 422, "self_payment"

        if not is_valid_amount(amount):
            return 422, "validation_failed"
        amount = int(amount)

        if not isinstance(note, str) or len(note) > 200:
            return 422, "validation_failed"

        if visibility not in ("public", "private"):
            return 422, "validation_failed"

        if effective_at is not None:
            effective_dt = parse_rfc3339(effective_at)
            if effective_dt is None or effective_dt > datetime.now(timezone.utc):
                return 422, "validation_failed"

        to_uid = STATE["by_handle"].get(to_handle)
        if not to_uid:
            return 404, "not_found"
        recipient = STATE["users"][to_uid]

        sender = STATE["users"][user["id"]]
        if sender["balance"] < amount:
            return 409, "insufficient_funds"

        payment_id = f"p_{uuid.uuid4().hex[:8]}"
        recorded_at = now_iso()
        effective_at = effective_at or recorded_at
        payment_obj = {
            "payment_id": payment_id,
            "from_user_id": sender["id"],
            "from_handle": sender["handle"],
            "to_user_id": recipient["id"],
            "to_handle": recipient["handle"],
            "amount": amount,
            "currency": STATE["currency"],
            "note": note,
            "visibility": visibility,
            "request_id": None,
            "settlement_id": None,
            "created_at": recorded_at,
            "effective_at": effective_at,
            "recorded_at": recorded_at,
            "revisions": [{
                "revision": 1, "amount": amount, "effective_at": effective_at,
                "recorded_at": recorded_at, "reason": ""
            }]
        }
        try:
            commit_transfers([{
                "transaction_id": f"tx_{payment_id}", "payment_id": payment_id,
                "from_user_id": sender["id"], "to_user_id": recipient["id"],
                "amount": amount, "effective_at": effective_at, "recorded_at": recorded_at
            }])
        except ValueError as error:
            return 409, str(error)
        STATE["payments"].append(payment_obj)
        return 201, payment_obj

    def exec_payment_correction(self, payment_id: str, user, body) -> tuple[int, any]:
        """追加修订和差额分录，不覆盖原 payment 或既有账本历史。"""
        payment = next((p for p in STATE["payments"] if p["payment_id"] == payment_id), None)
        if not payment:
            return 404, "not_found"
        if payment["from_user_id"] != user["id"]:
            return 403, "forbidden"

        revisions = payment.setdefault("revisions", [{
            "revision": 1, "amount": payment["amount"],
            "effective_at": payment.get("effective_at", payment["created_at"]),
            "recorded_at": payment.get("recorded_at", payment["created_at"]), "reason": ""
        }])
        expected = body.get("expected_revision")
        amount = body.get("amount")
        effective_at = body.get("effective_at")
        reason = body.get("reason")
        if (isinstance(expected, bool) or not isinstance(expected, int)
                or expected != revisions[-1]["revision"]):
            return 409, "stale_revision"
        if isinstance(amount, bool) or not isinstance(amount, int) or not 0 <= amount <= 1_000_000_000:
            return 422, "validation_failed"
        effective_dt = parse_rfc3339(effective_at)
        if (effective_dt is None or effective_dt > datetime.now(timezone.utc)
                or not isinstance(reason, str) or not 1 <= len(reason) <= 200):
            return 422, "validation_failed"

        delta = amount - revisions[-1]["amount"]
        recorded_dt = datetime.now(timezone.utc)
        last_recorded = parse_rfc3339(revisions[-1]["recorded_at"])
        if last_recorded is not None and recorded_dt <= last_recorded:
            recorded_dt = last_recorded + timedelta(microseconds=1)
        recorded_at = recorded_dt.isoformat()

        if delta:
            from_id = payment["from_user_id"] if delta > 0 else payment["to_user_id"]
            to_id = payment["to_user_id"] if delta > 0 else payment["from_user_id"]
            try:
                commit_transfers([{
                    "transaction_id": f"tx_corr_{payment_id}_{expected + 1}",
                    "payment_id": payment_id, "from_user_id": from_id, "to_user_id": to_id,
                    "amount": abs(delta), "effective_at": effective_at,
                    "recorded_at": recorded_at, "kind": "correction"
                }])
            except ValueError as error:
                return 409, str(error)

        revision = {
            "revision": expected + 1, "amount": amount, "effective_at": effective_at,
            "recorded_at": recorded_at, "reason": reason, "delta": delta
        }
        revisions.append(revision)
        return 201, revision

    def exec_request(self, user, body) -> tuple[int, any]:
        if "payer_handle" not in body or "amount" not in body:
            return 422, "validation_failed"

        payer_handle = body.get("payer_handle")
        amount = body.get("amount")
        note = body.get("note", "")

        if not isinstance(payer_handle, str):
            return 422, "validation_failed"
        if not re.fullmatch(r"^[a-z0-9_]{1,20}$", payer_handle):
            return 404, "not_found"

        if payer_handle == user["handle"]:
            return 422, "self_request"

        if not is_valid_amount(amount):
            return 422, "validation_failed"
        amount = int(amount)

        if not isinstance(note, str) or len(note) > 200:
            return 422, "validation_failed"

        payer_uid = STATE["by_handle"].get(payer_handle)
        if not payer_uid:
            return 404, "not_found"
        payer = STATE["users"][payer_uid]

        request_id = f"rq_{uuid.uuid4().hex[:8]}"
        created_at = now_iso()
        request_obj = {
            "request_id": request_id,
            "requester_id": user["id"],
            "requester_handle": user["handle"],
            "payer_id": payer["id"],
            "payer_handle": payer["handle"],
            "amount": amount,
            "currency": STATE["currency"],
            "note": note,
            "status": "pending",
            "payment_id": None,
            "created_at": created_at
        }
        STATE["requests"].append(request_obj)
        return 201, request_obj

    def exec_request_pay(self, request_id: str, user, body) -> tuple[int, any]:
        rq = next((r for r in STATE["requests"] if r["request_id"] == request_id), None)
        if not rq:
            return 404, "not_found"

        if rq["payer_id"] != user["id"]:
            return 403, "forbidden"

        if rq["status"] != "pending":
            return 409, "request_not_pending"

        visibility = body.get("visibility", "public")
        if visibility not in ("public", "private"):
            return 422, "validation_failed"

        amount = rq["amount"]
        payer = STATE["users"][user["id"]]
        if payer["balance"] < amount:
            return 409, "insufficient_funds"

        requester = STATE["users"][rq["requester_id"]]

        payment_id = f"p_{uuid.uuid4().hex[:8]}"
        created_at = now_iso()
        payment_obj = {
            "payment_id": payment_id,
            "from_user_id": payer["id"],
            "from_handle": payer["handle"],
            "to_user_id": requester["id"],
            "to_handle": requester["handle"],
            "amount": amount,
            "currency": STATE["currency"],
            "note": rq["note"],
            "visibility": visibility,
            "request_id": rq["request_id"],
            "settlement_id": None,
            "created_at": created_at,
            "effective_at": created_at,
            "recorded_at": created_at,
            "revisions": [{
                "revision": 1, "amount": amount, "effective_at": created_at,
                "recorded_at": created_at, "reason": ""
            }]
        }
        commit_transfers([{
            "transaction_id": f"tx_{payment_id}", "payment_id": payment_id,
            "from_user_id": payer["id"], "to_user_id": requester["id"],
            "amount": amount, "effective_at": created_at, "recorded_at": created_at,
            "kind": "request_payment"
        }])
        STATE["payments"].append(payment_obj)

        rq["status"] = "paid"
        rq["payment_id"] = payment_id

        return 201, payment_obj

    def exec_split(self, user, body) -> tuple[int, any]:
        if "amount" not in body or "participant_handles" not in body:
            return 422, "validation_failed"

        amount = body.get("amount")
        handles = body.get("participant_handles")
        note = body.get("note", "")

        if not is_valid_amount(amount):
            return 422, "validation_failed"
        amount = int(amount)

        if not isinstance(handles, list) or len(handles) == 0:
            return 422, "validation_failed"

        # Duplicate check
        if len(handles) != len(set(handles)):
            return 422, "validation_failed"

        # Validate all handles exist
        for h in handles:
            if not isinstance(h, str) or h not in STATE["by_handle"]:
                return 404, "not_found"

        if not isinstance(note, str) or len(note) > 200:
            return 422, "validation_failed"

        # Split rounding (§9)
        n = len(handles)
        base = amount // n
        remainder = amount - base * n
        shares = []
        for i, h in enumerate(handles):
            sh_amt = base + (1 if i < remainder else 0)
            shares.append({"handle": h, "amount": sh_amt})

        split_id = f"sp_{uuid.uuid4().hex[:8]}"
        created_at = now_iso()
        created_requests = []

        for item in shares:
            h = item["handle"]
            sh_amt = item["amount"]
            if h == user["handle"]:
                continue
            payer_uid = STATE["by_handle"][h]
            payer = STATE["users"][payer_uid]

            req_obj = {
                "request_id": f"rq_{uuid.uuid4().hex[:8]}",
                "requester_id": user["id"],
                "requester_handle": user["handle"],
                "payer_id": payer["id"],
                "payer_handle": payer["handle"],
                "amount": sh_amt,
                "currency": STATE["currency"],
                "note": note,
                "status": "pending",
                "payment_id": None,
                "created_at": created_at
            }
            STATE["requests"].append(req_obj)
            created_requests.append(req_obj)

        return 201, {
            "split_id": split_id,
            "amount": amount,
            "currency": STATE["currency"],
            "note": note,
            "shares": shares,
            "requests": created_requests,
            "created_at": created_at
        }

    def exec_settlement(self, user, body) -> tuple[int, any]:
        if user["id"] not in STATE["settlement_operator_ids"]:
            return 403, "forbidden"

        transfers = body.get("transfers")
        if not isinstance(transfers, list) or len(transfers) < 1 or len(transfers) > 32:
            return 422, "validation_failed"

        # Validate entries in input order
        validated_transfers = []
        net_deltas = {}

        for item in transfers:
            if not isinstance(item, dict):
                return 422, "validation_failed"

            from_h = item.get("from_handle")
            to_h = item.get("to_handle")
            amt = item.get("amount")
            note = item.get("note", "")
            vis = item.get("visibility", "public")

            if not isinstance(from_h, str) or not isinstance(to_h, str):
                return 422, "validation_failed"

            from_uid = STATE["by_handle"].get(from_h)
            to_uid = STATE["by_handle"].get(to_h)
            if not from_uid or not to_uid:
                return 404, "not_found"

            if from_h == to_h:
                return 422, "self_payment"

            if not is_valid_amount(amt):
                return 422, "validation_failed"
            amt = int(amt)

            if not isinstance(note, str) or len(note) > 200:
                return 422, "validation_failed"

            if vis not in ("public", "private"):
                return 422, "validation_failed"

            validated_transfers.append({
                "from_uid": from_uid,
                "from_handle": from_h,
                "to_uid": to_uid,
                "to_handle": to_h,
                "amount": amt,
                "note": note,
                "visibility": vis
            })

            net_deltas[from_uid] = net_deltas.get(from_uid, 0) - amt
            net_deltas[to_uid] = net_deltas.get(to_uid, 0) + amt

        # Collective affordability check
        for uid, delta in net_deltas.items():
            if STATE["users"][uid]["balance"] + delta < 0:
                return 409, "insufficient_funds"

        settlement_id = f"st_{uuid.uuid4().hex[:8]}"
        committed_at = now_iso()
        payments_out = []
        ledger_transfers = []

        for item in validated_transfers:
            payment_obj = {
                "payment_id": f"p_{uuid.uuid4().hex[:8]}",
                "from_user_id": item["from_uid"],
                "from_handle": item["from_handle"],
                "to_user_id": item["to_uid"],
                "to_handle": item["to_handle"],
                "amount": item["amount"],
                "currency": STATE["currency"],
                "note": item["note"],
                "visibility": item["visibility"],
                "request_id": None,
                "settlement_id": settlement_id,
                "created_at": committed_at,
                "effective_at": committed_at,
                "recorded_at": committed_at,
                "revisions": [{
                    "revision": 1, "amount": item["amount"], "effective_at": committed_at,
                    "recorded_at": committed_at, "reason": ""
                }]
            }
            payments_out.append(payment_obj)
            ledger_transfers.append({
                "transaction_id": f"tx_{payment_obj['payment_id']}",
                "payment_id": payment_obj["payment_id"],
                "from_user_id": item["from_uid"], "to_user_id": item["to_uid"],
                "amount": item["amount"], "effective_at": committed_at,
                "recorded_at": committed_at, "kind": "settlement"
            })

        try:
            commit_transfers(ledger_transfers)
        except ValueError:
            return 409, "insufficient_funds"
        STATE["payments"].extend(payments_out)

        return 201, {
            "settlement_id": settlement_id,
            "committed_at": committed_at,
            "payments": payments_out
        }

    # =========================================================================
    # Request State Transitions (Decline & Cancel)
    # =========================================================================

    def handle_request_decline(self, request_id: str, user):
        with STATE_LOCK:
            rq = next((r for r in STATE["requests"] if r["request_id"] == request_id), None)
            if not rq:
                return self.fail(404, "not_found")

            if rq["payer_id"] != user["id"]:
                return self.fail(403, "forbidden")

            if rq["status"] == "declined":
                return self.send_json(200, rq)

            if rq["status"] in ("paid", "cancelled"):
                return self.fail(409, "request_not_pending")

            rq["status"] = "declined"
            return self.send_json(200, rq)

    def handle_request_cancel(self, request_id: str, user):
        with STATE_LOCK:
            rq = next((r for r in STATE["requests"] if r["request_id"] == request_id), None)
            if not rq:
                return self.fail(404, "not_found")

            if rq["requester_id"] != user["id"]:
                return self.fail(403, "forbidden")

            if rq["status"] == "cancelled":
                return self.send_json(200, rq)

            if rq["status"] in ("paid", "declined"):
                return self.fail(409, "request_not_pending")

            rq["status"] = "cancelled"
            return self.send_json(200, rq)


class Server(ThreadingHTTPServer):
    request_queue_size = 256
    daemon_threads = True


def main() -> None:
    port = int(os.environ.get("PORT", "8080"))
    print(f"pocketful stage-1 server listening on 0.0.0.0:{port}", flush=True)
    Server(("0.0.0.0", port), Handler).serve_forever()


if __name__ == "__main__":
    main()
