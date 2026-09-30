"""Pocketful Stage 2 Server — bitemporal P2P payments, wallet engine, authorizations & Web UI."""
from __future__ import annotations

import functools
import hashlib
import http.cookies
import json
import os
import re
import sys
import threading
import uuid
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse, parse_qs

from passwords import hash_password, verify_password


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


STATE_LOCK = threading.RLock()

STATE = {
    "currency": "EUR",
    "minor_units": 2,
    "authorization_ttl_seconds": 600,
    "settlement_operator_ids": set(),
    "users": {},            # user_id -> user dict {"id", "email", "password", "display_name", "handle", "balance"}
    "by_handle": {},        # handle -> user_id
    "by_email": {},         # email.lower() -> user_id
    "tokens": {},           # token -> user_id
    "payments": [],         # list of payment dicts
    "requests": [],         # list of request dicts
    "authorizations": [],   # list of authorization dicts
    "idempotency": {},      # (user_id, method, path, key) -> {"canonical_body": ..., "response": ..., "status": ...}
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def format_money(minor: int, minor_units: int, currency: str) -> str:
    if minor_units == 0:
        return f"{minor} {currency}"
    text = str(minor).rjust(minor_units + 1, "0")
    return f"{text[:-minor_units]}.{text[-minor_units:]} {currency}"


def check_and_update_authorizations_expiry(now_str: str | None = None) -> None:
    """Must be called under STATE_LOCK."""
    if now_str is None:
        now_str = now_iso()
    for a in STATE["authorizations"]:
        if a["status"] == "open" and a["expires_at"] <= now_str:
            a["status"] = "expired"
            a["remaining_amount"] = 0


def get_user_balances(user_id: str, now_str: str | None = None) -> tuple[int, int, int]:
    """Must be called under STATE_LOCK. Returns (total, available, held)."""
    check_and_update_authorizations_expiry(now_str)
    user = STATE["users"].get(user_id)
    if not user:
        return 0, 0, 0
    total = user["balance"]
    held = sum(
        a.get("remaining_amount", a["amount"] - a.get("captured_amount", 0))
        for a in STATE["authorizations"]
        if a["from_user_id"] == user_id and a["status"] == "open"
    )
    available = max(0, total - held)
    return total, available, held


# =============================================================================
# Web UI HTML Template (Self-contained SPA)
# =============================================================================

INDEX_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Pocketful</title>
<style>
  :root {
    --bg: #0f172a;
    --surface: #1e293b;
    --surface-hover: #334155;
    --border: #334155;
    --text: #f8fafc;
    --text-muted: #94a3b8;
    --primary: #38bdf8;
    --primary-hover: #0ea5e9;
    --success: #34d399;
    --danger: #f87171;
    --warning: #fbbf24;
    --font: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: var(--bg);
    color: var(--text);
    font-family: var(--font);
    line-height: 1.5;
    min-height: 100vh;
    padding-bottom: 3rem;
  }
  header {
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    padding: 1rem 1.5rem;
    display: flex;
    justify-content: space-between;
    align-items: center;
    flex-wrap: wrap;
    gap: 1rem;
  }
  .brand { font-size: 1.25rem; font-weight: 700; color: var(--primary); text-decoration: none; }
  nav { display: flex; gap: 1rem; align-items: center; }
  nav a { color: var(--text-muted); text-decoration: none; font-size: 0.95rem; font-weight: 500; }
  nav a:hover, nav a.active { color: var(--text); }
  .user-badge { display: flex; align-items: center; gap: 0.75rem; font-size: 0.9rem; }
  .container { max-width: 900px; margin: 2rem auto; padding: 0 1rem; }
  .card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 1.5rem;
    margin-bottom: 1.5rem;
  }
  .balance-card { text-align: center; padding: 2rem; }
  .balance-label { font-size: 0.875rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.05em; }
  .balance-headline { font-size: 2.75rem; font-weight: 700; color: var(--text); margin: 0.5rem 0; }
  .balance-secondary { display: flex; justify-content: center; gap: 2rem; color: var(--text-muted); font-size: 0.95rem; margin-top: 0.5rem; }
  .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 1.5rem; }
  @media (max-width: 768px) { .grid-2 { grid-template-columns: 1fr; } }
  h2, h3 { font-size: 1.25rem; font-weight: 600; margin-bottom: 1rem; }
  .form-group { margin-bottom: 1rem; }
  label { display: block; font-size: 0.85rem; color: var(--text-muted); margin-bottom: 0.25rem; }
  input, select, textarea {
    width: 100%;
    padding: 0.65rem 0.85rem;
    background: #0b1120;
    border: 1px solid var(--border);
    border-radius: 6px;
    color: var(--text);
    font-size: 0.95rem;
    outline: none;
  }
  input:focus, select:focus, textarea:focus { border-color: var(--primary); }
  button {
    padding: 0.65rem 1.25rem;
    background: var(--primary);
    color: #0b1120;
    border: none;
    border-radius: 6px;
    font-weight: 600;
    cursor: pointer;
    font-size: 0.95rem;
    transition: background 0.15s;
  }
  button:hover { background: var(--primary-hover); }
  .btn-sm { padding: 0.35rem 0.75rem; font-size: 0.85rem; }
  .btn-danger { background: var(--danger); color: white; }
  .btn-secondary { background: var(--surface-hover); color: var(--text); }
  .error-box { background: rgba(248, 113, 113, 0.15); border: 1px solid var(--danger); color: var(--danger); padding: 0.75rem; border-radius: 6px; font-size: 0.9rem; margin-bottom: 1rem; }
  .uncertain-box { background: rgba(251, 191, 36, 0.15); border: 1px solid var(--warning); color: var(--warning); padding: 0.75rem; border-radius: 6px; font-size: 0.9rem; margin-bottom: 1rem; }
  .empty-state { text-align: center; padding: 2.5rem; color: var(--text-muted); font-size: 0.95rem; }
  .list-item {
    border-bottom: 1px solid var(--border);
    padding: 1rem 0;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
  .list-item:last-child { border-bottom: none; }
  .badge {
    padding: 0.2rem 0.5rem;
    border-radius: 4px;
    font-size: 0.75rem;
    font-weight: 600;
    text-transform: uppercase;
  }
  .badge-pending { background: rgba(251, 191, 36, 0.2); color: var(--warning); }
  .badge-paid, .badge-captured { background: rgba(52, 211, 153, 0.2); color: var(--success); }
  .badge-declined, .badge-cancelled, .badge-voided { background: rgba(248, 113, 113, 0.2); color: var(--danger); }
  .badge-expired { background: rgba(148, 163, 184, 0.2); color: var(--text-muted); }
  .split-preview-box { background: #0b1120; border: 1px solid var(--border); border-radius: 6px; padding: 1rem; margin-top: 1rem; }
</style>
</head>
<body>
<header>
  <div style="display:flex;align-items:center;gap:1.5rem;">
    <a href="/" class="brand">Pocketful</a>
    <nav id="nav-links">
      <a href="/" id="nav-wallet">Wallet</a>
      <a href="/requests" id="nav-requests">Requests</a>
      <a href="/split" id="nav-split">Split</a>
      <a href="/authorizations" id="nav-authorizations">Authorizations</a>
    </nav>
  </div>
  <div id="auth-header"></div>
</header>

<main class="container" id="app-root"></main>

<script>
let currentCurrency = "EUR";
let currentMinorUnits = 2;
let currentUser = null;
let currentToken = localStorage.getItem("pocketful_token") || getCookie("token") || "";

let payFormKey = null;
let payFormSnapshot = null;
let authorizeFormKey = null;
let authorizeFormSnapshot = null;
let latestRefreshSeq = 0;

function getCookie(name) {
  const match = document.cookie.match(new RegExp('(^| )' + name + '=([^;]+)'));
  return match ? match[2] : "";
}

function setToken(token) {
  currentToken = token;
  if (token) {
    localStorage.setItem("pocketful_token", token);
    document.cookie = "token=" + token + "; path=/; max-age=86400";
  } else {
    localStorage.removeItem("pocketful_token");
    document.cookie = "token=; path=/; max-age=0";
  }
}

function formatMoney(minor, minorUnits, currency) {
  if (minorUnits === 0) return `${minor} ${currency}`;
  const text = String(minor).padStart(minorUnits + 1, '0');
  const intPart = text.slice(0, text.length - minorUnits);
  const fracPart = text.slice(text.length - minorUnits);
  return `${intPart}.${fracPart} ${currency}`;
}

function parseDecimalToMinor(valStr, minorUnits) {
  if (typeof valStr !== 'string') return null;
  valStr = valStr.trim();
  if (!/^[0-9]+(\\.[0-9]+)?$/.test(valStr)) return null;
  const parts = valStr.split('.');
  const intPart = parseInt(parts[0], 10);
  const fracStr = parts[1] || '';
  if (fracStr.length > minorUnits) return null; // Non-rounded overflow
  const paddedFrac = fracStr.padEnd(minorUnits, '0');
  const fracPart = minorUnits > 0 ? parseInt(paddedFrac, 10) : 0;
  const total = intPart * Math.pow(10, minorUnits) + fracPart;
  if (total < 1 || total > 1000000000) return null;
  return total;
}

async function apiRequest(path, options = {}) {
  options.headers = options.headers || {};
  if (currentToken) {
    options.headers["Authorization"] = "Bearer " + currentToken;
  }
  if (options.body && typeof options.body === 'object') {
    options.headers["Content-Type"] = "application/json";
    options.body = JSON.stringify(options.body);
  }
  return fetch(path, options);
}

function logout() {
  setToken("");
  currentUser = null;
  updateHeaderAuth();
  navigate("/login");
}

function navigate(url) {
  window.history.pushState({}, "", url);
  route();
}

window.addEventListener("popstate", route);

async function route() {
  const path = window.location.pathname;
  updateNav();

  if (!currentToken && path !== "/login" && path !== "/signup") {
    navigate("/login");
    return;
  }

  if (currentToken && !currentUser) {
    try {
      const res = await apiRequest("/me");
      if (res.ok) {
        currentUser = await res.json();
        currentCurrency = currentUser.currency;
        currentMinorUnits = currentUser.minor_units;
      } else {
        setToken("");
        navigate("/login");
        return;
      }
    } catch (e) {
      // offline / retryable
    }
  }

  updateHeaderAuth();

  const root = document.getElementById("app-root");
  if (path === "/login") {
    renderLogin(root);
  } else if (path === "/signup") {
    renderSignup(root);
  } else if (path === "/requests") {
    renderRequests(root);
  } else if (path === "/split") {
    renderSplit(root);
  } else if (path === "/authorizations") {
    renderAuthorizations(root);
  } else {
    renderWallet(root);
  }
}

function updateNav() {
  const path = window.location.pathname;
  document.querySelectorAll("nav a").forEach(a => {
    a.classList.toggle("active", a.getAttribute("href") === path);
  });
}

function updateHeaderAuth() {
  const container = document.getElementById("auth-header");
  if (!container) return;
  if (currentUser) {
    container.innerHTML = `
      <div class="user-badge">
        <span>Signed in as <strong data-testid="current-user">${currentUser.display_name}</strong> (<span data-testid="current-handle">${currentUser.handle}</span>)</span>
        <button class="btn-sm btn-secondary" data-testid="logout-button" onclick="logout()">Sign out</button>
      </div>
    `;
  } else {
    container.innerHTML = `
      <div id="logged-out-view">
        <a href="/login" style="color:var(--primary);text-decoration:none;font-weight:600;margin-right:1rem;">Sign in</a>
        <a href="/signup" style="color:var(--text-muted);text-decoration:none;">Sign up</a>
      </div>
    `;
  }
}

// ----------------------------------------------------------------------------
// Screens: Login & Signup
// ----------------------------------------------------------------------------

function renderLogin(root) {
  root.innerHTML = `
    <div class="card" style="max-width:400px;margin:2rem auto;">
      <h2>Sign in to Pocketful</h2>
      <div id="login-error-container"></div>
      <form onsubmit="handleLoginSubmit(event)">
        <div class="form-group">
          <label>Email</label>
          <input type="email" data-testid="login-email" required>
        </div>
        <div class="form-group">
          <label>Password</label>
          <input type="password" data-testid="login-password" required>
        </div>
        <button type="submit" data-testid="login-submit" style="width:100%;">Sign In</button>
      </form>
    </div>
  `;
}

async function handleLoginSubmit(e) {
  e.preventDefault();
  const email = document.querySelector('[data-testid="login-email"]').value;
  const password = document.querySelector('[data-testid="login-password"]').value;
  const errContainer = document.getElementById("login-error-container");
  errContainer.innerHTML = "";

  try {
    const res = await apiRequest("/auth/login", {
      method: "POST",
      body: { email, password }
    });
    if (res.ok) {
      const data = await res.json();
      setToken(data.token);
      currentUser = null;
      navigate("/");
    } else {
      errContainer.innerHTML = `<div class="error-box" data-testid="auth-error">Invalid email or password</div>`;
    }
  } catch (err) {
    errContainer.innerHTML = `<div class="error-box" data-testid="auth-error">Network error. Please try again.</div>`;
  }
}

function renderSignup(root) {
  root.innerHTML = `
    <div class="card" style="max-width:400px;margin:2rem auto;">
      <h2>Create your account</h2>
      <div id="signup-error-container"></div>
      <form onsubmit="handleSignupSubmit(event)">
        <div class="form-group">
          <label>Display Name</label>
          <input type="text" data-testid="signup-display-name" required>
        </div>
        <div class="form-group">
          <label>Email</label>
          <input type="email" data-testid="signup-email" required>
        </div>
        <div class="form-group">
          <label>Password</label>
          <input type="password" data-testid="signup-password" required>
        </div>
        <button type="submit" data-testid="signup-submit" style="width:100%;">Create Account</button>
      </form>
    </div>
  `;
}

async function handleSignupSubmit(e) {
  e.preventDefault();
  const display_name = document.querySelector('[data-testid="signup-display-name"]').value;
  const email = document.querySelector('[data-testid="signup-email"]').value;
  const password = document.querySelector('[data-testid="signup-password"]').value;
  const errContainer = document.getElementById("signup-error-container");
  errContainer.innerHTML = "";

  try {
    const res = await apiRequest("/auth/signup", {
      method: "POST",
      body: { email, password, display_name }
    });
    if (res.ok) {
      const data = await res.json();
      setToken(data.token);
      currentUser = null;
      navigate("/");
    } else {
      errContainer.innerHTML = `<div class="error-box" data-testid="auth-error">Signup failed</div>`;
    }
  } catch (err) {
    errContainer.innerHTML = `<div class="error-box" data-testid="auth-error">Network error</div>`;
  }
}

// ----------------------------------------------------------------------------
// Screen: Wallet (Balance, Pay, Request, Activity Feed)
// ----------------------------------------------------------------------------

function renderWallet(root) {
  root.innerHTML = `
    <div class="card balance-card">
      <div class="balance-label">Available to Spend</div>
      <div class="balance-headline" data-testid="wallet-available" data-amount="0">0.00 EUR</div>
      <div class="balance-secondary">
        <div>Total: <span data-testid="wallet-balance" data-amount="0">0.00 EUR</span></div>
        <div id="wallet-held-container"></div>
      </div>
      <div style="margin-top:1.25rem;">
        <button class="btn-sm btn-secondary" data-testid="wallet-refresh" onclick="refreshWallet()">Refresh Balance</button>
      </div>
    </div>

    <div class="grid-2">
      <!-- Pay Form -->
      <div class="card">
        <h3>Pay</h3>
        <div id="pay-feedback"></div>
        <form onsubmit="handlePaySubmit(event)">
          <div class="form-group">
            <label>Recipient Handle</label>
            <input type="text" data-testid="pay-handle" oninput="onPayFieldChanged()" required>
          </div>
          <div class="form-group">
            <label>Amount (${currentCurrency})</label>
            <input type="text" data-testid="pay-amount" placeholder="e.g. 15.00" oninput="onPayFieldChanged()" required>
          </div>
          <div class="form-group">
            <label>Note</label>
            <input type="text" data-testid="pay-note" oninput="onPayFieldChanged()">
          </div>
          <div class="form-group">
            <label>Visibility</label>
            <select data-testid="pay-visibility" onchange="onPayFieldChanged()">
              <option value="public" selected>Public</option>
              <option value="private">Private</option>
            </select>
          </div>
          <button type="submit" data-testid="pay-submit" style="width:100%;">Send Payment</button>
        </form>
      </div>

      <!-- Request Form -->
      <div class="card">
        <h3>Request Money</h3>
        <div id="request-feedback"></div>
        <form onsubmit="handleRequestSubmit(event)">
          <div class="form-group">
            <label>Payer Handle</label>
            <input type="text" data-testid="request-handle" required>
          </div>
          <div class="form-group">
            <label>Amount (${currentCurrency})</label>
            <input type="text" data-testid="request-amount" placeholder="e.g. 15.00" required>
          </div>
          <div class="form-group">
            <label>Note</label>
            <input type="text" data-testid="request-note">
          </div>
          <button type="submit" data-testid="request-submit" style="width:100%;">Create Request</button>
        </form>
      </div>
    </div>

    <div class="card">
      <h3>Activity Feed</h3>
      <div id="activity-container">
        <div class="empty-state" data-testid="empty-activity">No payments yet</div>
      </div>
    </div>
  `;

  refreshWallet();
}

function getPayFormSnapshot() {
  const h = document.querySelector('[data-testid="pay-handle"]')?.value || "";
  const a = document.querySelector('[data-testid="pay-amount"]')?.value || "";
  const n = document.querySelector('[data-testid="pay-note"]')?.value || "";
  const v = document.querySelector('[data-testid="pay-visibility"]')?.value || "public";
  return JSON.stringify({h, a, n, v});
}

function onPayFieldChanged() {
  const currentSnap = getPayFormSnapshot();
  if (currentSnap !== payFormSnapshot) {
    payFormKey = crypto.randomUUID();
    payFormSnapshot = currentSnap;
  }
}

async function refreshWallet() {
  const seq = ++latestRefreshSeq;
  try {
    const [meRes, actRes] = await Promise.all([
      apiRequest("/me"),
      apiRequest("/activity?limit=100")
    ]);
    if (seq !== latestRefreshSeq) return; // Out-of-order drop

    if (meRes.ok) {
      const me = await meRes.json();
      currentUser = me;
      currentCurrency = me.currency;
      currentMinorUnits = me.minor_units;
      updateHeaderAuth();

      const elAvail = document.querySelector('[data-testid="wallet-available"]');
      const elBal = document.querySelector('[data-testid="wallet-balance"]');
      const heldContainer = document.getElementById("wallet-held-container");

      if (elAvail) {
        elAvail.textContent = formatMoney(me.available, me.minor_units, me.currency);
        elAvail.setAttribute("data-amount", String(me.available));
      }
      if (elBal) {
        elBal.textContent = formatMoney(me.total, me.minor_units, me.currency);
        elBal.setAttribute("data-amount", String(me.total));
      }
      if (heldContainer) {
        if (me.held > 0) {
          heldContainer.innerHTML = `Held: <span data-testid="wallet-held" data-amount="${me.held}">${formatMoney(me.held, me.minor_units, me.currency)}</span>`;
        } else {
          heldContainer.innerHTML = "";
        }
      }
    }

    if (actRes.ok) {
      const act = await actRes.json();
      renderActivityFeed(act.payments || []);
    }
  } catch (err) {}
}

function renderActivityFeed(payments) {
  const container = document.getElementById("activity-container");
  if (!container) return;
  if (payments.length === 0) {
    container.innerHTML = `<div class="empty-state" data-testid="empty-activity">No activity yet</div>`;
    return;
  }

  let html = `<div data-testid="activity-list">`;
  for (const p of payments) {
    html += `
      <div class="list-item" data-testid="activity-item-${p.payment_id}" data-visibility="${p.visibility}">
        <div>
          <div style="font-weight:600;" data-testid="activity-parties-${p.payment_id}">${p.from_handle} &rarr; ${p.to_handle}</div>
          <div style="font-size:0.85rem;color:var(--text-muted);" data-testid="activity-note-${p.payment_id}">${p.note || ""}</div>
        </div>
        <div style="text-align:right;">
          <div style="font-weight:700;" data-testid="activity-amount-${p.payment_id}">${formatMoney(p.amount, currentMinorUnits, currentCurrency)}</div>
          <span class="badge ${p.visibility === 'private' ? 'badge-pending' : 'badge-paid'}">${p.visibility}</span>
        </div>
      </div>
    `;
  }
  html += `</div>`;
  container.innerHTML = html;
}

async function handlePaySubmit(e) {
  e.preventDefault();
  const feedback = document.getElementById("pay-feedback");
  const handle = document.querySelector('[data-testid="pay-handle"]').value.trim();
  const amountStr = document.querySelector('[data-testid="pay-amount"]').value.trim();
  const note = document.querySelector('[data-testid="pay-note"]').value;
  const visibility = document.querySelector('[data-testid="pay-visibility"]').value;

  const minor = parseDecimalToMinor(amountStr, currentMinorUnits);
  if (minor === null) {
    feedback.innerHTML = `<div class="error-box" data-testid="pay-error">Invalid amount</div>`;
    return;
  }

  if (!payFormKey) {
    payFormKey = crypto.randomUUID();
    payFormSnapshot = getPayFormSnapshot();
  }

  try {
    const res = await apiRequest("/payments", {
      method: "POST",
      headers: { "Idempotency-Key": payFormKey },
      body: { to_handle: handle, amount: minor, note, visibility }
    });

    if (res.ok) {
      feedback.innerHTML = "";
      await refreshWallet();
    } else {
      const err = await res.json();
      feedback.innerHTML = `<div class="error-box" data-testid="pay-error">${err.error?.code || 'Payment failed'}</div>`;
      await refreshWallet();
    }
  } catch (err) {
    feedback.innerHTML = `<div class="uncertain-box" data-testid="pay-uncertain">Payment status uncertain. You may retry.</div>`;
  }
}

async function handleRequestSubmit(e) {
  e.preventDefault();
  const feedback = document.getElementById("request-feedback");
  const handle = document.querySelector('[data-testid="request-handle"]').value.trim();
  const amountStr = document.querySelector('[data-testid="request-amount"]').value.trim();
  const note = document.querySelector('[data-testid="request-note"]').value;

  const minor = parseDecimalToMinor(amountStr, currentMinorUnits);
  if (minor === null) {
    feedback.innerHTML = `<div class="error-box" data-testid="request-error">Invalid amount</div>`;
    return;
  }

  try {
    const res = await apiRequest("/requests", {
      method: "POST",
      headers: { "Idempotency-Key": crypto.randomUUID() },
      body: { payer_handle: handle, amount: minor, note }
    });

    if (res.ok) {
      feedback.innerHTML = "";
      document.querySelector('[data-testid="request-handle"]').value = "";
      document.querySelector('[data-testid="request-amount"]').value = "";
      document.querySelector('[data-testid="request-note"]').value = "";
    } else {
      const err = await res.json();
      feedback.innerHTML = `<div class="error-box" data-testid="request-error">${err.error?.code || 'Request failed'}</div>`;
    }
  } catch (err) {
    feedback.innerHTML = `<div class="error-box" data-testid="request-error">Network error</div>`;
  }
}

// ----------------------------------------------------------------------------
// Screen: Requests
// ----------------------------------------------------------------------------

function renderRequests(root) {
  root.innerHTML = `
    <div class="card">
      <h2>Requests</h2>
      <div id="request-action-error"></div>
      <div id="requests-display">
        <div class="grid-2">
          <div>
            <h3>Incoming</h3>
            <div id="incoming-container" data-testid="incoming-list"></div>
          </div>
          <div>
            <h3>Outgoing</h3>
            <div id="outgoing-container" data-testid="outgoing-list"></div>
          </div>
        </div>
      </div>
      <div id="empty-requests-container"></div>
    </div>
  `;
  refreshRequests();
}

async function refreshRequests() {
  try {
    const res = await apiRequest("/requests?limit=100");
    if (!res.ok) return;
    const data = await res.json();
    const reqs = data.requests || [];

    const incoming = reqs.filter(r => r.payer_id === currentUser.user_id);
    const outgoing = reqs.filter(r => r.requester_id === currentUser.user_id);

    const emptyContainer = document.getElementById("empty-requests-container");
    const display = document.getElementById("requests-display");

    if (incoming.length === 0 && outgoing.length === 0) {
      if (emptyContainer) emptyContainer.innerHTML = `<div class="empty-state" data-testid="empty-requests">No requests</div>`;
      if (display) display.style.display = "block";
    } else {
      if (emptyContainer) emptyContainer.innerHTML = "";
      if (display) display.style.display = "block";
    }

    const inContainer = document.getElementById("incoming-container");
    const outContainer = document.getElementById("outgoing-container");

    if (inContainer) {
      inContainer.innerHTML = incoming.map(r => `
        <div class="list-item" data-testid="request-item-${r.request_id}" data-status="${r.status}">
          <div>
            <div>From <strong>@${r.requester_handle}</strong></div>
            <div style="font-weight:700;" data-testid="request-amount-${r.request_id}">${formatMoney(r.amount, currentMinorUnits, currentCurrency)}</div>
            <div style="font-size:0.85rem;color:var(--text-muted);">${r.note || ""}</div>
          </div>
          <div style="display:flex;gap:0.5rem;align-items:center;">
            <span class="badge badge-${r.status}">${r.status}</span>
            ${r.status === 'pending' ? `
              <button class="btn-sm" data-testid="request-pay-${r.request_id}" onclick="actRequestPay('${r.request_id}')">Pay</button>
              <button class="btn-sm btn-secondary" data-testid="request-decline-${r.request_id}" onclick="actRequestDecline('${r.request_id}')">Decline</button>
            ` : ''}
          </div>
        </div>
      `).join("");
    }

    if (outContainer) {
      outContainer.innerHTML = outgoing.map(r => `
        <div class="list-item" data-testid="request-item-${r.request_id}" data-status="${r.status}">
          <div>
            <div>To <strong>@${r.payer_handle}</strong></div>
            <div style="font-weight:700;" data-testid="request-amount-${r.request_id}">${formatMoney(r.amount, currentMinorUnits, currentCurrency)}</div>
            <div style="font-size:0.85rem;color:var(--text-muted);">${r.note || ""}</div>
          </div>
          <div style="display:flex;gap:0.5rem;align-items:center;">
            <span class="badge badge-${r.status}">${r.status}</span>
            ${r.status === 'pending' ? `
              <button class="btn-sm btn-danger" data-testid="request-cancel-${r.request_id}" onclick="actRequestCancel('${r.request_id}')">Cancel</button>
            ` : ''}
          </div>
        </div>
      `).join("");
    }
  } catch (err) {}
}

async function actRequestPay(id) {
  const errBox = document.getElementById("request-action-error");
  errBox.innerHTML = "";
  try {
    const res = await apiRequest(`/requests/${id}/pay`, {
      method: "POST",
      headers: { "Idempotency-Key": crypto.randomUUID() },
      body: {}
    });
    if (res.ok) {
      await refreshRequests();
    } else {
      errBox.innerHTML = `<div class="error-box" data-testid="request-error">Payment refused</div>`;
      await refreshRequests();
    }
  } catch (e) {
    errBox.innerHTML = `<div class="error-box" data-testid="request-error">Network error</div>`;
  }
}

async function actRequestDecline(id) {
  const errBox = document.getElementById("request-action-error");
  errBox.innerHTML = "";
  try {
    const res = await apiRequest(`/requests/${id}/decline`, { method: "POST" });
    if (res.ok) {
      await refreshRequests();
    } else {
      errBox.innerHTML = `<div class="error-box" data-testid="request-error">Decline refused</div>`;
      await refreshRequests();
    }
  } catch (e) {
    errBox.innerHTML = `<div class="error-box" data-testid="request-error">Network error</div>`;
  }
}

async function actRequestCancel(id) {
  const errBox = document.getElementById("request-action-error");
  errBox.innerHTML = "";
  try {
    const res = await apiRequest(`/requests/${id}/cancel`, { method: "POST" });
    if (res.ok) {
      await refreshRequests();
    } else {
      errBox.innerHTML = `<div class="error-box" data-testid="request-error">Cancel refused</div>`;
      await refreshRequests();
    }
  } catch (e) {
    errBox.innerHTML = `<div class="error-box" data-testid="request-error">Network error</div>`;
  }
}

// ----------------------------------------------------------------------------
// Screen: Split Form
// ----------------------------------------------------------------------------

function renderSplit(root) {
  root.innerHTML = `
    <div class="card" style="max-width:550px;margin:2rem auto;">
      <h2>Split a Bill</h2>
      <div id="split-feedback"></div>
      <form onsubmit="handleSplitSubmit(event)">
        <div class="form-group">
          <label>Total Amount (${currentCurrency})</label>
          <input type="text" data-testid="split-amount" placeholder="e.g. 30.00" oninput="updateSplitPreview()" required>
        </div>
        <div class="form-group">
          <label>Participants (comma-separated handles in order)</label>
          <input type="text" data-testid="split-handles" placeholder="e.g. ada, bob, cy" oninput="updateSplitPreview()" required>
        </div>
        <div class="form-group">
          <label>Note</label>
          <input type="text" data-testid="split-note">
        </div>
        <div id="split-preview-container"></div>
        <button type="submit" data-testid="split-submit" style="width:100%;margin-top:1rem;">Submit Split</button>
      </form>
    </div>
  `;
}

function updateSplitPreview() {
  const amtStr = document.querySelector('[data-testid="split-amount"]')?.value || "";
  const handlesStr = document.querySelector('[data-testid="split-handles"]')?.value || "";
  const container = document.getElementById("split-preview-container");
  if (!container) return;

  const handles = handlesStr.split(',').map(s => s.trim()).filter(Boolean);
  const minor = parseDecimalToMinor(amtStr, currentMinorUnits);

  if (minor === null || handles.length === 0) {
    container.innerHTML = "";
    return;
  }

  const n = handles.length;
  const base = Math.floor(minor / n);
  const rem = minor - base * n;

  let html = `<div class="split-preview-box" data-testid="split-preview">
    <div style="font-weight:600;font-size:0.85rem;margin-bottom:0.5rem;color:var(--text-muted);">Split Preview</div>`;
  for (let i = 0; i < n; i++) {
    const share = base + (i < rem ? 1 : 0);
    html += `
      <div style="display:flex;justify-content:space-between;padding:0.25rem 0;">
        <span>@${handles[i]}</span>
        <strong data-testid="split-share-${handles[i]}">${formatMoney(share, currentMinorUnits, currentCurrency)}</strong>
      </div>
    `;
  }
  html += `</div>`;
  container.innerHTML = html;
}

async function handleSplitSubmit(e) {
  e.preventDefault();
  const feedback = document.getElementById("split-feedback");
  const amtStr = document.querySelector('[data-testid="split-amount"]').value;
  const handlesStr = document.querySelector('[data-testid="split-handles"]').value;
  const note = document.querySelector('[data-testid="split-note"]').value;

  const minor = parseDecimalToMinor(amtStr, currentMinorUnits);
  if (minor === null) {
    feedback.innerHTML = `<div class="error-box" data-testid="split-error">Invalid amount</div>`;
    return;
  }

  const handles = handlesStr.split(',').map(s => s.trim()).filter(Boolean);
  if (handles.length === 0) {
    feedback.innerHTML = `<div class="error-box" data-testid="split-error">Enter at least one handle</div>`;
    return;
  }

  try {
    const res = await apiRequest("/splits", {
      method: "POST",
      headers: { "Idempotency-Key": crypto.randomUUID() },
      body: { amount: minor, participant_handles: handles, note }
    });

    if (res.ok) {
      feedback.innerHTML = "";
      navigate("/requests");
    } else {
      const err = await res.json();
      feedback.innerHTML = `<div class="error-box" data-testid="split-error">${err.error?.code || 'Split refused'}</div>`;
    }
  } catch (err) {
    feedback.innerHTML = `<div class="error-box" data-testid="split-error">Network error</div>`;
  }
}

// ----------------------------------------------------------------------------
// Screen: Authorizations
// ----------------------------------------------------------------------------

function renderAuthorizations(root) {
  root.innerHTML = `
    <div class="card">
      <h2>Authorizations & Holds</h2>
      <div id="auth-action-error"></div>
      
      <!-- Authorize Form -->
      <div style="background:#0b1120;border:1px solid var(--border);border-radius:8px;padding:1.25rem;margin-bottom:1.5rem;">
        <h3>Create Hold</h3>
        <div id="authorize-feedback"></div>
        <form onsubmit="handleAuthorizeSubmit(event)">
          <div class="grid-2">
            <div class="form-group">
              <label>Recipient Handle</label>
              <input type="text" data-testid="authorize-handle" oninput="onAuthorizeFieldChanged()" required>
            </div>
            <div class="form-group">
              <label>Amount (${currentCurrency})</label>
              <input type="text" data-testid="authorize-amount" placeholder="e.g. 20.00" oninput="onAuthorizeFieldChanged()" required>
            </div>
          </div>
          <div class="grid-2">
            <div class="form-group">
              <label>Note</label>
              <input type="text" data-testid="authorize-note" oninput="onAuthorizeFieldChanged()">
            </div>
            <div class="form-group">
              <label>Visibility</label>
              <select data-testid="authorize-visibility" onchange="onAuthorizeFieldChanged()">
                <option value="public" selected>Public</option>
                <option value="private">Private</option>
              </select>
            </div>
          </div>
          <button type="submit" data-testid="authorize-submit">Authorize Funds</button>
        </form>
      </div>

      <!-- Authorizations List -->
      <h3>Active and Past Authorizations</h3>
      <div id="authorizations-container"></div>
    </div>
  `;

  refreshAuthorizations();
}

function getAuthorizeFormSnapshot() {
  const h = document.querySelector('[data-testid="authorize-handle"]')?.value || "";
  const a = document.querySelector('[data-testid="authorize-amount"]')?.value || "";
  const n = document.querySelector('[data-testid="authorize-note"]')?.value || "";
  const v = document.querySelector('[data-testid="authorize-visibility"]')?.value || "public";
  return JSON.stringify({h, a, n, v});
}

function onAuthorizeFieldChanged() {
  const snap = getAuthorizeFormSnapshot();
  if (snap !== authorizeFormSnapshot) {
    authorizeFormKey = crypto.randomUUID();
    authorizeFormSnapshot = snap;
  }
}

async function handleAuthorizeSubmit(e) {
  e.preventDefault();
  const feedback = document.getElementById("authorize-feedback");
  const handle = document.querySelector('[data-testid="authorize-handle"]').value.trim();
  const amountStr = document.querySelector('[data-testid="authorize-amount"]').value.trim();
  const note = document.querySelector('[data-testid="authorize-note"]').value;
  const visibility = document.querySelector('[data-testid="authorize-visibility"]').value;

  const minor = parseDecimalToMinor(amountStr, currentMinorUnits);
  if (minor === null) {
    feedback.innerHTML = `<div class="error-box" data-testid="authorize-error">Invalid amount</div>`;
    return;
  }

  if (!authorizeFormKey) {
    authorizeFormKey = crypto.randomUUID();
    authorizeFormSnapshot = getAuthorizeFormSnapshot();
  }

  try {
    const res = await apiRequest("/authorizations", {
      method: "POST",
      headers: { "Idempotency-Key": authorizeFormKey },
      body: { to_handle: handle, amount: minor, note, visibility }
    });

    if (res.ok) {
      feedback.innerHTML = "";
      await refreshAuthorizations();
    } else {
      const err = await res.json();
      feedback.innerHTML = `<div class="error-box" data-testid="authorize-error">${err.error?.code || 'Authorization refused'}</div>`;
    }
  } catch (err) {
    feedback.innerHTML = `<div class="error-box" data-testid="authorize-error">Network error</div>`;
  }
}

async function refreshAuthorizations() {
  const container = document.getElementById("authorizations-container");
  if (!container) return;

  try {
    const res = await apiRequest("/authorizations?limit=100");
    if (!res.ok) return;
    const data = await res.json();
    const auths = data.authorizations || [];

    if (auths.length === 0) {
      container.innerHTML = `<div class="empty-state" data-testid="empty-authorizations">No authorizations yet</div>`;
      return;
    }

    let html = `<div data-testid="authorization-list">`;
    for (const a of auths) {
      const isPayer = (a.from_user_id === currentUser.user_id);
      const isReceiver = (a.to_user_id === currentUser.user_id);
      const remaining = (a.status === 'open') ? (a.amount - (a.captured_amount || 0)) : 0;
      const remainingDecimal = currentMinorUnits > 0 ? (remaining / Math.pow(10, currentMinorUnits)).toFixed(currentMinorUnits) : String(remaining);

      html += `
        <div class="list-item" data-testid="authorization-item-${a.authorization_id}" data-status="${a.status}">
          <div>
            <div style="font-weight:600;">
              ${isPayer ? `To <strong>@${a.to_handle}</strong>` : `From <strong>@${a.from_handle}</strong>`}
            </div>
            <div>
              Authorized: <span style="font-weight:700;" data-testid="authorization-amount-${a.authorization_id}">${formatMoney(a.amount, currentMinorUnits, currentCurrency)}</span>
              ${a.status === 'captured' ? ` | Captured: <span data-testid="authorization-captured-${a.authorization_id}">${formatMoney(a.captured_amount, currentMinorUnits, currentCurrency)}</span>` : ''}
            </div>
            <div style="font-size:0.85rem;color:var(--text-muted);">Expires: <span data-testid="authorization-expires-${a.authorization_id}">${a.expires_at}</span></div>
          </div>
          <div style="display:flex;gap:0.5rem;align-items:center;">
            <span class="badge badge-${a.status}">${a.status}</span>
            ${isReceiver && a.status === 'open' ? `
              <div style="display:flex;gap:0.35rem;align-items:center;">
                <input type="text" data-testid="authorization-capture-amount-${a.authorization_id}" value="${remainingDecimal}" style="width:80px;padding:0.25rem 0.5rem;font-size:0.85rem;">
                <button class="btn-sm" data-testid="authorization-capture-${a.authorization_id}" onclick="actCapture('${a.authorization_id}')">Capture</button>
              </div>
            ` : ''}
            ${isPayer && a.status === 'open' ? `
              <button class="btn-sm btn-secondary" data-testid="authorization-void-${a.authorization_id}" onclick="actVoid('${a.authorization_id}')">Void</button>
            ` : ''}
          </div>
        </div>
      `;
    }
    html += `</div>`;
    container.innerHTML = html;
  } catch (err) {}
}

async function actCapture(id) {
  const errBox = document.getElementById("auth-action-error");
  errBox.innerHTML = "";
  const inputEl = document.querySelector(`[data-testid="authorization-capture-amount-${id}"]`);
  const amtStr = inputEl?.value.trim() || "";
  const minor = parseDecimalToMinor(amtStr, currentMinorUnits);
  if (minor === null) {
    errBox.innerHTML = `<div class="error-box" data-testid="authorization-error">Invalid capture amount</div>`;
    return;
  }

  try {
    const res = await apiRequest(`/authorizations/${id}/capture`, {
      method: "POST",
      headers: { "Idempotency-Key": crypto.randomUUID() },
      body: { amount: minor }
    });
    if (res.ok) {
      await refreshAuthorizations();
    } else {
      const err = await res.json();
      errBox.innerHTML = `<div class="error-box" data-testid="authorization-error">${err.error?.code || 'Capture refused'}</div>`;
      await refreshAuthorizations();
    }
  } catch (err) {
    errBox.innerHTML = `<div class="error-box" data-testid="authorization-error">Network error</div>`;
  }
}

async function actVoid(id) {
  const errBox = document.getElementById("auth-action-error");
  errBox.innerHTML = "";
  try {
    const res = await apiRequest(`/authorizations/${id}/void`, { method: "POST" });
    if (res.ok) {
      await refreshAuthorizations();
    } else {
      const err = await res.json();
      errBox.innerHTML = `<div class="error-box" data-testid="authorization-error">${err.error?.code || 'Void refused'}</div>`;
      await refreshAuthorizations();
    }
  } catch (err) {
    errBox.innerHTML = `<div class="error-box" data-testid="authorization-error">Network error</div>`;
  }
}

// Initial bootstrap
route();
</script>
</body>
</html>
"""


# =============================================================================
# Request Handler
# =============================================================================

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

    def send_html(self, status: int, html_str: str) -> None:
        body = html_str.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
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
        # 1. Bearer Header
        auth = self.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            token = auth[7:].strip()
            user_id = STATE["tokens"].get(token)
            if user_id:
                return STATE["users"].get(user_id)

        # 2. Cookie fallback
        cookie_header = self.headers.get("Cookie", "")
        if cookie_header:
            c = http.cookies.SimpleCookie()
            try:
                c.load(cookie_header)
                if "token" in c:
                    token = c["token"].value
                    user_id = STATE["tokens"].get(token)
                    if user_id:
                        return STATE["users"].get(user_id)
            except Exception:
                pass
        return None

    def do_GET(self):
        self.route("GET")

    def do_POST(self):
        self.route("POST")

    def route(self, method: str):
        parsed_url = urlparse(self.path)
        path = parsed_url.path.rstrip("/") or "/"
        accept = self.headers.get("Accept", "")

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

        # HTML UI Routes
        if method == "GET":
            if path in ("/", "/login", "/signup", "/split"):
                return self.send_html(200, INDEX_HTML)

            if path in ("/requests", "/authorizations") and "text/html" in accept:
                return self.send_html(200, INDEX_HTML)

        # All other endpoints require authentication
        user = self.get_auth_user()
        if not user:
            return self.fail(401, "unauthenticated")

        # GET API endpoints
        if method == "GET":
            if path == "/me":
                return self.handle_me(user)
            if path == "/activity":
                return self.handle_activity(user, parsed_url.query)
            if path == "/requests":
                return self.handle_requests_list(user, parsed_url.query)
            if path == "/authorizations":
                return self.handle_authorizations_list(user, parsed_url.query)
            return self.fail(404, "not_found")

        # POST endpoints
        if method == "POST":
            # Idempotent write paths
            idempotent_paths = (
                "/payments",
                "/requests",
                "/splits",
                "/settlements",
                "/authorizations"
            )
            is_idem = (
                path in idempotent_paths
                or (path.startswith("/requests/") and path.endswith("/pay"))
                or (path.startswith("/authorizations/") and path.endswith("/capture"))
            )
            if is_idem:
                return self.handle_idempotent_post(method, path, user)

            # Non-idempotent write paths
            if path.startswith("/requests/") and path.endswith("/decline"):
                request_id = path.split("/")[2]
                return self.handle_request_decline(request_id, user)

            if path.startswith("/requests/") and path.endswith("/cancel"):
                request_id = path.split("/")[2]
                return self.handle_request_cancel(request_id, user)

            if path.startswith("/authorizations/") and path.endswith("/void"):
                auth_id = path.split("/")[2]
                return self.handle_authorization_void(auth_id, user)

            return self.fail(404, "not_found")

    # =========================================================================
    # Test Fixtures & Harness
    # =========================================================================

    def handle_reset(self):
        fixture = self.read_body()
        if fixture is None or not isinstance(fixture, dict):
            return self.fail(400, "malformed_request")

        # Validate users & balances
        for u in fixture.get("users", []):
            if not isinstance(u.get("handle"), str) or not re.fullmatch(r"^[a-z0-9_]{1,20}$", u["handle"]):
                return self.fail(422, "validation_failed")
            if u.get("balance", 0) < 0:
                return self.fail(422, "validation_failed")

        # Validate authorizations & sum of unexpired open holds <= balance
        auths_fixture = fixture.get("authorizations", [])
        now_str = now_iso()
        user_balances_map = {u["id"]: int(u.get("balance", 0)) for u in fixture.get("users", [])}
        user_holds_map = {u["id"]: 0 for u in fixture.get("users", [])}

        for a in auths_fixture:
            amt = int(a.get("amount", 0))
            if amt < 1 or amt > 1_000_000_000:
                return self.fail(422, "validation_failed")
            fid = a.get("from_user_id")
            status = a.get("status", "open")
            expires_at = a.get("expires_at", "")
            if status == "open" and expires_at > now_str:
                if fid in user_holds_map:
                    user_holds_map[fid] += amt

        for uid, total_bal in user_balances_map.items():
            if user_holds_map[uid] > total_bal:
                return self.fail(422, "validation_failed")

        ttl = fixture.get("authorization_ttl_seconds", 600)
        if not isinstance(ttl, int) or ttl <= 0:
            return self.fail(422, "validation_failed")

        with STATE_LOCK:
            STATE["currency"] = fixture.get("currency", "EUR")
            STATE["minor_units"] = fixture.get("minor_units", 2)
            STATE["authorization_ttl_seconds"] = ttl
            STATE["settlement_operator_ids"] = set(fixture.get("settlement_operator_ids", []))
            STATE["users"] = {}
            STATE["by_handle"] = {}
            STATE["by_email"] = {}
            STATE["tokens"] = {}
            STATE["payments"] = []
            STATE["requests"] = []
            STATE["authorizations"] = []
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
                    "balance": int(u.get("balance", 0))
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
                    "authorization_id": p.get("authorization_id"),
                    "settlement_id": p.get("settlement_id"),
                    "created_at": p.get("created_at") or "2026-09-01T00:00:00+00:00"
                }
                STATE["payments"].append(pm)

            for r in fixture.get("requests", []):
                req_u = STATE["users"].get(r["requester_user_id"])
                pay_u = STATE["users"].get(r["payer_user_id"])
                rq = {
                    "request_id": r.get("id") or f"rq_{uuid.uuid4().hex[:8]}",
                    "requester_id": r["requester_user_id"],
                    "requester_handle": req_u["handle"] if req_u else "",
                    "payer_id": r["payer_user_id"],
                    "payer_handle": pay_u["handle"] if pay_u else "",
                    "amount": int(r["amount"]),
                    "currency": STATE["currency"],
                    "note": r.get("note", ""),
                    "status": r.get("status", "pending"),
                    "created_at": r.get("created_at") or "2026-09-01T00:00:00+00:00",
                    "payment_id": r.get("payment_id")
                }
                STATE["requests"].append(rq)

            for a in auths_fixture:
                from_u = STATE["users"].get(a["from_user_id"])
                to_u = STATE["users"].get(a["to_user_id"])
                status = a.get("status", "open")
                expires_at = a.get("expires_at") or now_str
                amt = int(a["amount"])
                cap_amt = int(a.get("captured_amount", 0))
                p_id = a.get("payment_id")
                p_ids = a.get("payment_ids", ([p_id] if p_id else []))

                rem = (amt - cap_amt) if status == "open" else 0
                if status == "open" and expires_at <= now_str:
                    status = "expired"
                    rem = 0

                au = {
                    "authorization_id": a.get("id") or f"a_{uuid.uuid4().hex[:8]}",
                    "from_user_id": a["from_user_id"],
                    "from_handle": from_u["handle"] if from_u else "",
                    "to_user_id": a["to_user_id"],
                    "to_handle": to_u["handle"] if to_u else "",
                    "amount": amt,
                    "captured_amount": cap_amt,
                    "remaining_amount": rem,
                    "currency": STATE["currency"],
                    "note": a.get("note", ""),
                    "visibility": a.get("visibility", "public"),
                    "status": status,
                    "expires_at": expires_at,
                    "payment_id": p_id,
                    "payment_ids": p_ids,
                    "created_at": a.get("created_at") or now_str
                }
                STATE["authorizations"].append(au)

        return self.send_json(204, None)

    def handle_export(self):
        with STATE_LOCK:
            check_and_update_authorizations_expiry()
            idem_export = {}
            for k, v in STATE["idempotency"].items():
                key_str = json.dumps(list(k))
                idem_export[key_str] = v

            state_snapshot = {
                "currency": STATE["currency"],
                "minor_units": STATE["minor_units"],
                "authorization_ttl_seconds": STATE.get("authorization_ttl_seconds", 600),
                "settlement_operator_ids": list(STATE["settlement_operator_ids"]),
                "users": dict(STATE["users"]),
                "by_handle": dict(STATE["by_handle"]),
                "by_email": dict(STATE["by_email"]),
                "tokens": dict(STATE["tokens"]),
                "payments": list(STATE["payments"]),
                "requests": list(STATE["requests"]),
                "authorizations": list(STATE["authorizations"]),
                "idempotency": idem_export,
            }
        return self.send_json(200, {
            "track": "pocketful",
            "format_version": 1,
            "state": state_snapshot
        })

    def handle_import(self):
        data = self.read_body()
        if data is None or not isinstance(data, dict):
            return self.fail(400, "malformed_request")

        s = data.get("state", data)
        if not isinstance(s, dict):
            return self.fail(422, "validation_failed")

        with STATE_LOCK:
            STATE["currency"] = s.get("currency", "EUR")
            STATE["minor_units"] = s.get("minor_units", 2)
            STATE["authorization_ttl_seconds"] = s.get("authorization_ttl_seconds", 600)
            STATE["settlement_operator_ids"] = set(s.get("settlement_operator_ids", []))
            STATE["users"] = {}
            STATE["by_handle"] = {}
            STATE["by_email"] = {}

            raw_users = s.get("users", {})
            if isinstance(raw_users, dict):
                for uid, u in raw_users.items():
                    STATE["users"][uid] = dict(u)
                    STATE["by_handle"][u["handle"]] = uid
                    STATE["by_email"][u["email"].lower()] = uid
            elif isinstance(raw_users, list):
                for u in raw_users:
                    uid = u["id"]
                    STATE["users"][uid] = dict(u)
                    STATE["by_handle"][u["handle"]] = uid
                    STATE["by_email"][u["email"].lower()] = uid

            if "by_handle" in s and isinstance(s["by_handle"], dict):
                STATE["by_handle"].update(s["by_handle"])
            if "by_email" in s and isinstance(s["by_email"], dict):
                STATE["by_email"].update(s["by_email"])

            STATE["tokens"] = dict(s.get("tokens", {}))
            STATE["payments"] = [dict(p) for p in s.get("payments", [])]
            STATE["requests"] = [dict(r) for r in s.get("requests", [])]
            STATE["authorizations"] = [dict(a) for a in s.get("authorizations", [])]

            STATE["idempotency"] = {}
            raw_idem = s.get("idempotency", {})
            if isinstance(raw_idem, dict):
                for k_str, v in raw_idem.items():
                    try:
                        k_tuple = tuple(json.loads(k_str))
                        STATE["idempotency"][k_tuple] = v
                    except Exception:
                        pass
            elif isinstance(raw_idem, list):
                for item in raw_idem:
                    try:
                        k_tuple = tuple(item["token"])
                        STATE["idempotency"][k_tuple] = {
                            "canonical_body": item["canonical_body"],
                            "response": item["response"],
                            "status": item["status"]
                        }
                    except Exception:
                        pass

            check_and_update_authorizations_expiry()

        return self.send_json(204, None)

    # =========================================================================
    # Authentication
    # =========================================================================

    def handle_signup(self):
        data = self.read_body()
        if data is None or not isinstance(data, dict):
            return self.fail(400, "malformed_request")

        email = data.get("email")
        password = data.get("password")
        display_name = data.get("display_name")
        handle = data.get("handle")

        if not isinstance(email, str) or not isinstance(password, str):
            return self.fail(400, "malformed_request")

        if "@" not in email:
            return self.fail(422, "validation_failed")

        if handle is None:
            local_part = email.split("@")[0].lower()
            derived_handle = re.sub(r"[^a-z0-9_]", "_", local_part)[:20]
        else:
            if not isinstance(handle, str) or not re.fullmatch(r"^[a-z0-9_]{1,20}$", handle):
                return self.fail(422, "validation_failed")
            derived_handle = handle

        if not derived_handle:
            return self.fail(422, "validation_failed")

        with STATE_LOCK:
            if email.lower() in STATE["by_email"] or derived_handle in STATE["by_handle"]:
                return self.fail(409, "user_already_exists")

            user_id = f"u_{derived_handle}_{uuid.uuid4().hex[:4]}"
            user = {
                "id": user_id,
                "email": email,
                "password": hash_password(password),
                "display_name": display_name if isinstance(display_name, str) else derived_handle,
                "handle": derived_handle,
                "balance": 0
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
            total, available, held = get_user_balances(user["id"])
            current_user = STATE["users"].get(user["id"])
            return self.send_json(200, {
                "user_id": current_user["id"],
                "display_name": current_user["display_name"],
                "handle": current_user["handle"],
                "balance": total,
                "total": total,
                "available": available,
                "held": held,
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
            visible = []
            for p in reversed(STATE["payments"]):
                is_party = (p["from_user_id"] == caller_id or p["to_user_id"] == caller_id)
                if p["visibility"] == "public" or is_party:
                    visible.append(p)

            page = visible[offset: offset + limit]
            has_more = (offset + limit) < len(visible)

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

            page = filtered[offset: offset + limit]
            has_more = (offset + limit) < len(filtered)

        return self.send_json(200, {
            "requests": page,
            "has_more": has_more
        })

    def handle_authorizations_list(self, user, query_str: str):
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
            if status not in ("open", "captured", "voided", "expired"):
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
            check_and_update_authorizations_expiry()
            caller_id = user["id"]
            filtered = []
            for a in reversed(STATE["authorizations"]):
                is_payer = (a["from_user_id"] == caller_id)
                is_receiver = (a["to_user_id"] == caller_id)

                if not (is_payer or is_receiver):
                    continue

                if direction == "incoming" and not is_receiver:
                    continue
                if direction == "outgoing" and not is_payer:
                    continue

                if status and a["status"] != status:
                    continue

                filtered.append(a)

            page = filtered[offset: offset + limit]
            has_more = (offset + limit) < len(filtered)

        return self.send_json(200, {
            "authorizations": page,
            "has_more": has_more
        })

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
                    return self.send_json(200, record["response"])
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
            elif path == "/authorizations":
                status, resp = self.exec_authorization(user, body)
            elif path.startswith("/requests/") and path.endswith("/pay"):
                request_id = path.split("/")[2]
                status, resp = self.exec_request_pay(request_id, user, body)
            elif path.startswith("/authorizations/") and path.endswith("/capture"):
                auth_id = path.split("/")[2]
                status, resp = self.exec_authorization_capture(auth_id, user, body)
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

        to_uid = STATE["by_handle"].get(to_handle)
        if not to_uid:
            return 404, "not_found"
        recipient = STATE["users"][to_uid]

        sender = STATE["users"][user["id"]]
        _, available, _ = get_user_balances(sender["id"])
        if available < amount:
            return 409, "insufficient_funds"

        # Transfer funds
        sender["balance"] -= amount
        recipient["balance"] += amount

        payment_id = f"p_{uuid.uuid4().hex[:8]}"
        created_at = now_iso()
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
            "authorization_id": None,
            "settlement_id": None,
            "created_at": created_at
        }
        STATE["payments"].append(payment_obj)
        return 201, payment_obj

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
            "created_at": created_at,
            "payment_id": None
        }
        STATE["requests"].append(request_obj)
        return 201, request_obj

    def exec_request_pay(self, request_id: str, user, body) -> tuple[int, any]:
        visibility = body.get("visibility", "public")
        if visibility not in ("public", "private"):
            return 422, "validation_failed"

        req = next((r for r in STATE["requests"] if r["request_id"] == request_id), None)
        if not req:
            return 404, "not_found"

        if user["id"] != req["payer_id"]:
            return 403, "forbidden"

        if req["status"] != "pending":
            return 409, "request_not_pending"

        payer = STATE["users"][req["payer_id"]]
        requester = STATE["users"][req["requester_id"]]
        amount = req["amount"]

        _, available, _ = get_user_balances(payer["id"])
        if available < amount:
            return 409, "insufficient_funds"

        # Transfer funds
        payer["balance"] -= amount
        requester["balance"] += amount

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
            "note": req["note"],
            "visibility": visibility,
            "request_id": req["request_id"],
            "authorization_id": None,
            "settlement_id": None,
            "created_at": created_at
        }
        STATE["payments"].append(payment_obj)

        req["status"] = "paid"
        req["payment_id"] = payment_id

        return 201, payment_obj

    def handle_request_decline(self, request_id: str, user):
        with STATE_LOCK:
            req = next((r for r in STATE["requests"] if r["request_id"] == request_id), None)
            if not req:
                return self.fail(404, "not_found")

            if user["id"] != req["payer_id"]:
                return self.fail(403, "forbidden")

            if req["status"] != "pending":
                return self.fail(409, "request_not_pending")

            req["status"] = "declined"
            return self.send_json(200, req)

    def handle_request_cancel(self, request_id: str, user):
        with STATE_LOCK:
            req = next((r for r in STATE["requests"] if r["request_id"] == request_id), None)
            if not req:
                return self.fail(404, "not_found")

            if user["id"] != req["requester_id"]:
                return self.fail(403, "forbidden")

            if req["status"] != "pending":
                return self.fail(409, "request_not_pending")

            req["status"] = "cancelled"
            return self.send_json(200, req)

    def exec_split(self, user, body) -> tuple[int, any]:
        amount = body.get("amount")
        handles = body.get("participant_handles")
        note = body.get("note", "")

        if not is_valid_amount(amount):
            return 422, "validation_failed"
        amount = int(amount)

        if not isinstance(handles, list) or len(handles) == 0:
            return 422, "validation_failed"

        if len(set(handles)) != len(handles):
            return 422, "validation_failed"

        for h in handles:
            if not isinstance(h, str) or not re.fullmatch(r"^[a-z0-9_]{1,20}$", h):
                return 404, "not_found"
            if h not in STATE["by_handle"]:
                return 404, "not_found"

        if not isinstance(note, str) or len(note) > 200:
            return 422, "validation_failed"

        n = len(handles)
        base = amount // n
        remainder = amount - (base * n)

        shares = []
        for i, h in enumerate(handles):
            share_amt = base + (1 if i < remainder else 0)
            shares.append({"handle": h, "amount": share_amt})

        created_requests = []
        created_at = now_iso()
        for s in shares:
            target_handle = s["handle"]
            if target_handle == user["handle"]:
                continue
            payer_uid = STATE["by_handle"][target_handle]
            payer = STATE["users"][payer_uid]

            rq_id = f"rq_{uuid.uuid4().hex[:8]}"
            rq_obj = {
                "request_id": rq_id,
                "requester_id": user["id"],
                "requester_handle": user["handle"],
                "payer_id": payer["id"],
                "payer_handle": payer["handle"],
                "amount": s["amount"],
                "currency": STATE["currency"],
                "note": note,
                "status": "pending",
                "created_at": created_at,
                "payment_id": None
            }
            STATE["requests"].append(rq_obj)
            created_requests.append(rq_obj)

        return 201, {
            "shares": shares,
            "requests": created_requests
        }

    def exec_settlement(self, user, body) -> tuple[int, any]:
        if user["id"] not in STATE["settlement_operator_ids"]:
            return 403, "forbidden"

        transfers = body.get("transfers")
        note = body.get("note", "")

        if not isinstance(transfers, list) or len(transfers) == 0:
            return 422, "validation_failed"

        if not isinstance(note, str) or len(note) > 200:
            return 422, "validation_failed"

        user_net = {}
        for t in transfers:
            if not isinstance(t, dict):
                return 422, "validation_failed"
            from_h = t.get("from_handle")
            to_h = t.get("to_handle")
            amt = t.get("amount")

            if not isinstance(from_h, str) or from_h not in STATE["by_handle"]:
                return 404, "not_found"
            if not isinstance(to_h, str) or to_h not in STATE["by_handle"]:
                return 404, "not_found"

            if from_h == to_h:
                return 422, "validation_failed"

            if not is_valid_amount(amt):
                return 422, "validation_failed"
            amt = int(amt)

            from_uid = STATE["by_handle"][from_h]
            to_uid = STATE["by_handle"][to_h]

            user_net[from_uid] = user_net.get(from_uid, 0) - amt
            user_net[to_uid] = user_net.get(to_uid, 0) + amt

        for uid, net in user_net.items():
            if net < 0:
                _, available, _ = get_user_balances(uid)
                if available < (-net):
                    return 409, "insufficient_funds"

        settlement_id = f"s_{uuid.uuid4().hex[:8]}"
        created_at = now_iso()
        payment_ids = []

        for t in transfers:
            amt = int(t["amount"])
            from_uid = STATE["by_handle"][t["from_handle"]]
            to_uid = STATE["by_handle"][t["to_handle"]]
            sender = STATE["users"][from_uid]
            recipient = STATE["users"][to_uid]

            sender["balance"] -= amt
            recipient["balance"] += amt

            pid = f"p_{uuid.uuid4().hex[:8]}"
            pm = {
                "payment_id": pid,
                "from_user_id": sender["id"],
                "from_handle": sender["handle"],
                "to_user_id": recipient["id"],
                "to_handle": recipient["handle"],
                "amount": amt,
                "currency": STATE["currency"],
                "note": note,
                "visibility": "private",
                "request_id": None,
                "authorization_id": None,
                "settlement_id": settlement_id,
                "created_at": created_at
            }
            STATE["payments"].append(pm)
            payment_ids.append(pid)

        return 201, {
            "settlement_id": settlement_id,
            "transfers_count": len(transfers),
            "payment_ids": payment_ids,
            "created_at": created_at
        }

    # =========================================================================
    # Authorizations & Holds (Stage 2)
    # =========================================================================

    def exec_authorization(self, user, body) -> tuple[int, any]:
        if "to_handle" not in body or "amount" not in body:
            return 422, "validation_failed"

        to_handle = body.get("to_handle")
        amount = body.get("amount")
        note = body.get("note", "")
        visibility = body.get("visibility", "public")

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

        to_uid = STATE["by_handle"].get(to_handle)
        if not to_uid:
            return 404, "not_found"
        recipient = STATE["users"][to_uid]

        sender = STATE["users"][user["id"]]
        _, available, _ = get_user_balances(sender["id"])
        if available < amount:
            return 409, "insufficient_funds"

        ttl = STATE.get("authorization_ttl_seconds", 600)
        now_dt = datetime.now(timezone.utc)
        expires_at = (now_dt + timedelta(seconds=ttl)).isoformat()
        auth_id = f"a_{uuid.uuid4().hex[:8]}"

        auth_obj = {
            "authorization_id": auth_id,
            "from_user_id": sender["id"],
            "from_handle": sender["handle"],
            "to_user_id": recipient["id"],
            "to_handle": recipient["handle"],
            "amount": amount,
            "captured_amount": 0,
            "remaining_amount": amount,
            "currency": STATE["currency"],
            "note": note,
            "visibility": visibility,
            "status": "open",
            "expires_at": expires_at,
            "payment_id": None,
            "payment_ids": [],
            "created_at": now_dt.isoformat()
        }
        STATE["authorizations"].append(auth_obj)
        return 201, auth_obj

    def exec_authorization_capture(self, auth_id: str, user, body) -> tuple[int, any]:
        auth = next((a for a in STATE["authorizations"] if a["authorization_id"] == auth_id), None)
        if not auth:
            return 404, "not_found"

        if user["id"] != auth["to_user_id"]:
            return 403, "forbidden"

        now_str = now_iso()
        if auth["expires_at"] <= now_str:
            if auth["status"] == "open":
                auth["status"] = "expired"
                auth["remaining_amount"] = 0
            return 409, "authorization_expired"

        if auth["status"] != "open":
            return 409, "authorization_not_open"

        remaining = auth["amount"] - auth["captured_amount"]
        amount = body.get("amount", remaining)
        final = body.get("final", True)

        if amount is None:
            amount = remaining

        if not is_valid_amount(amount):
            return 422, "validation_failed"
        amount = int(amount)

        if not isinstance(final, bool):
            return 422, "validation_failed"

        if amount > remaining:
            return 422, "capture_exceeds_authorization"

        payer = STATE["users"][auth["from_user_id"]]
        receiver = STATE["users"][auth["to_user_id"]]

        # Transfer funds
        payer["balance"] -= amount
        receiver["balance"] += amount

        payment_id = f"p_{uuid.uuid4().hex[:8]}"
        payment_obj = {
            "payment_id": payment_id,
            "from_user_id": payer["id"],
            "from_handle": payer["handle"],
            "to_user_id": receiver["id"],
            "to_handle": receiver["handle"],
            "amount": amount,
            "currency": STATE["currency"],
            "note": auth["note"],
            "visibility": auth["visibility"],
            "request_id": None,
            "authorization_id": auth["authorization_id"],
            "settlement_id": None,
            "created_at": now_str
        }
        STATE["payments"].append(payment_obj)

        auth["captured_amount"] += amount
        auth["payment_id"] = payment_id
        if "payment_ids" not in auth:
            auth["payment_ids"] = []
        auth["payment_ids"].append(payment_id)

        remaining_now = auth["amount"] - auth["captured_amount"]
        if final or remaining_now == 0:
            auth["status"] = "captured"
            auth["remaining_amount"] = 0
        else:
            auth["status"] = "open"
            auth["remaining_amount"] = remaining_now

        return 201, payment_obj

    def handle_authorization_void(self, auth_id: str, user):
        with STATE_LOCK:
            auth = next((a for a in STATE["authorizations"] if a["authorization_id"] == auth_id), None)
            if not auth:
                return self.fail(404, "not_found")

            if user["id"] != auth["from_user_id"]:
                return self.fail(403, "forbidden")

            if auth["status"] == "voided":
                return self.send_json(200, auth)

            now_str = now_iso()
            if auth["expires_at"] <= now_str:
                auth["status"] = "expired"
                auth["remaining_amount"] = 0
                return self.fail(409, "authorization_not_open")

            if auth["status"] != "open":
                return self.fail(409, "authorization_not_open")

            auth["status"] = "voided"
            auth["remaining_amount"] = 0
            return self.send_json(200, auth)


def run_server():
    port = int(os.environ.get("PORT", "8080"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"Pocketful Stage 2 Server listening on port {port}...")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    run_server()
