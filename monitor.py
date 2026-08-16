#!/usr/bin/env python3
"""Scheduled sync task.
Polls a public data source on a schedule, diffs against saved state, and posts a
short summary to a notification endpoint when something new crosses a threshold.
State is persisted between runs. Endpoint comes from the NTFY_TOPIC env/secret."""
import json, os, shutil, subprocess, sys, time, urllib.request, urllib.error, datetime

HERE       = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(HERE, "state.json")
LOG_FILE   = os.path.join(HERE, "monitor.log")
def _topic():
    t = os.environ.get("NTFY_TOPIC", "").strip()
    if t: return t
    p = os.path.join(HERE, "topic.txt")
    return open(p).read().strip() if os.path.exists(p) else ""
TOPIC = _topic()

# Wallets to track. Add more dicts here to watch additional wallets.
WALLETS = [
    {"label": "DONJO",   "address": "5GmQHd4vQ2eeGHTr6ifEDYG8aHNxBiv14XK9cQvNvfGS"},
    {"label": "FrankDOG","address": "498g1rVnFcnjBjpfw1xyqA1WvgQXUU8RWuELjxkjAayQ"},
]
RPC      = "https://api.mainnet-beta.solana.com"
UA       = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
TOKEN_PROGRAMS = ["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                  "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"]  # SPL + Token-2022
QUOTE = {"So11111111111111111111111111111111111111112",   # WSOL
         "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",   # USDC
         "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"}   # USDT
THRESHOLD_USD   = 500.0
ALERT_EVERY_BUY = True    # alert on EVERY real (paid) buy, any size, new or existing.
ALERT_SELLS     = True    # alert when he swaps a token OUT for SOL/USDC (a sell), any size.
ALERT_MONEY_IN  = True    # alert when SOL/USDC/USDT ARRIVES (not part of a buy/sell) — funding in.
MONEYIN_USD_MIN = 1.0     # ignore sub-$ dust inflows / tiny sell proceeds.
SOL_MINT  = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
# Known Solana centralized-exchange hot/withdrawal wallets. Best-effort seed list — send me
# Solscan-labelled addresses to expand it. Money-in from these is labelled with the exchange name.
EXCHANGE_ADDRESSES = {
    "5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhUvuAi9": "Binance",
    "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM": "Binance",
    "2AQdpHJ2JpcEgPiATUXjQxA8QmafFegfQwSLWSprPicm": "Coinbase",
    "H8sMJSCQxfKiFTCfDR3DUMLPwcRbM61LGFJ8N4dK3WjS": "Coinbase",
    "GJRs4FwHtemZ5ZE9x3FNvJ8TMwitKTh21yxdRPqn7npE": "Coinbase",
    "FWznbcNXWQuHTawe9RxvQ2LdCENssh12dsznf4RiouN5": "Kraken",
    "AC5RDfQFmDS1deWZos921JfqscXdByf8BKHs5ACWjtW2": "Bybit",
}
_price_cache = {}

def log(msg):
    line = f"[{datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a") as f: f.write(line + "\n")
    except Exception: pass

def http_json(url, data=None, headers=None, timeout=30):
    h = {"User-Agent": UA}; h.update(headers or {})
    req = urllib.request.Request(url, data=data, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())

def rpc(method, params, retries=5):
    body = json.dumps({"jsonrpc":"2.0","id":1,"method":method,"params":params}).encode()
    for i in range(retries):
        try:
            d = http_json(RPC, data=body, headers={"Content-Type":"application/json"})
            if "result" in d: return d["result"]
            time.sleep(2*(i+1))
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
            time.sleep(2*(i+1))
    return None

def dexscreener(mint):
    """Return (symbol, priceUsd) for a mint, cached. (None, None) if not indexed."""
    if mint in _price_cache: return _price_cache[mint]
    res = (None, None)
    try:
        d = http_json("https://api.dexscreener.com/latest/dex/tokens/" + mint, timeout=20)
        pairs = d.get("pairs") or []
        if pairs:
            p = pairs[0]
            res = (p.get("baseToken", {}).get("symbol"), float(p["priceUsd"]) if p.get("priceUsd") else None)
    except Exception: pass
    _price_cache[mint] = res
    return res

def _blank_wallet_state():
    return {"seen": [], "known_mints": [], "initialized": False}

def load_state():
    st = {}
    if os.path.exists(STATE_FILE):
        try: st = json.load(open(STATE_FILE))
        except Exception: st = {}
    # Migrate legacy flat single-wallet state -> per-wallet under the first wallet (DONJO).
    if "wallets" not in st:
        legacy = {"seen": st.get("seen", []),
                  "known_mints": st.get("known_mints", []),
                  "initialized": st.get("initialized", False)}
        st = {"wallets": {}}
        if legacy["initialized"] or legacy["seen"] or legacy["known_mints"]:
            st["wallets"][WALLETS[0]["address"]] = legacy
            log(f"Migrated legacy state into wallet slot {WALLETS[0]['label']}.")
    for w in WALLETS:
        st["wallets"].setdefault(w["address"], _blank_wallet_state())
    return st

def save_state(st):
    for addr, ws in st["wallets"].items():
        ws["seen"] = ws["seen"][-1000:]
    json.dump(st, open(STATE_FILE, "w"))

def notify_phone(title, message, tags):
    if not TOPIC: log("no NTFY_TOPIC; cannot push"); return
    # HTTP headers must be latin-1; strip emoji/non-latin-1 from Title (the Tags field
    # still gives ntfy an emoji icon). Emoji stays intact in the UTF-8 body.
    safe_title = title.encode("latin-1", "ignore").decode("latin-1").strip() or "Wallet alert"
    try:
        urllib.request.urlopen(urllib.request.Request(
            "https://ntfy.sh/"+TOPIC, data=message.encode("utf-8"),
            headers={"Title": safe_title, "Priority": "high", "Tags": tags, "User-Agent": UA}), timeout=20)
    except Exception as e: log(f"phone push failed: {e}")

def notify_mac(title, message):
    if not shutil.which("osascript"): return
    t = title.replace('"',"'"); m = message.replace('"',"'")
    try: subprocess.run(["osascript","-e",f'display notification "{m}" with title "{t}" sound name "Glass"'], timeout=10)
    except Exception: pass

def owner_deltas(meta, wallet):
    """mint -> (post-pre) uiAmount for balances owned by wallet."""
    pre, post = {}, {}
    for tb in meta.get("preTokenBalances", []):
        if tb.get("owner")==wallet: pre[tb["mint"]] = tb["uiTokenAmount"]["uiAmount"] or 0.0
    for tb in meta.get("postTokenBalances", []):
        if tb.get("owner")==wallet: post[tb["mint"]] = tb["uiTokenAmount"]["uiAmount"] or 0.0
    return {m: post.get(m,0.0)-pre.get(m,0.0) for m in set(pre)|set(post)}

def native_sol_delta(tx, wallet):
    """wallet's native SOL change in this tx (negative = it spent SOL)."""
    m = tx["meta"]; keys = [k["pubkey"] for k in tx["transaction"]["message"]["accountKeys"]]
    if wallet in keys:
        i = keys.index(wallet)
        return (m["postBalances"][i] - m["preBalances"][i]) / 1e9
    return 0.0

def spl_sender(meta, wallet, mints):
    """Owner (≠wallet) whose balance in any of `mints` dropped most this tx = the sender."""
    pre, post = {}, {}
    for b in meta.get("preTokenBalances", []):
        if b.get("mint") in mints and b.get("owner"): pre[b["owner"]] = pre.get(b["owner"],0.0) + (b["uiTokenAmount"]["uiAmount"] or 0.0)
    for b in meta.get("postTokenBalances", []):
        if b.get("mint") in mints and b.get("owner"): post[b["owner"]] = post.get(b["owner"],0.0) + (b["uiTokenAmount"]["uiAmount"] or 0.0)
    cand, drop = None, -1e-9
    for o in set(pre)|set(post):
        if o == wallet: continue
        d = post.get(o,0.0) - pre.get(o,0.0)
        if d < drop: drop, cand = d, o
    return cand

def native_sender(tx, wallet):
    """AccountKey (≠wallet) whose native SOL dropped most this tx = the SOL sender."""
    m = tx["meta"]; keys = [k["pubkey"] for k in tx["transaction"]["message"]["accountKeys"]]
    cand, drop = None, -1e-9
    for i, k in enumerate(keys):
        if k == wallet: continue
        d = (m["postBalances"][i] - m["preBalances"][i]) / 1e9
        if d < drop: drop, cand = d, k
    return cand

def current_mints(wallet):
    """All mints the wallet currently holds (both token programs)."""
    mints = set()
    for prog in TOKEN_PROGRAMS:
        res = rpc("getTokenAccountsByOwner", [wallet, {"programId": prog}, {"encoding":"jsonParsed"}])
        if res:
            for a in res.get("value", []):
                try: mints.add(a["account"]["data"]["parsed"]["info"]["mint"])
                except Exception: pass
    return mints

def process_wallet(w, ws):
    """Scan one wallet; return list of alert dicts (each tagged with the wallet label)."""
    label, wallet = w["label"], w["address"]
    seen = set(ws.get("seen", [])); known = set(ws.get("known_mints", []))
    sigs = rpc("getSignaturesForAddress", [wallet, {"limit": 100}])
    if sigs is None: log(f"[{label}] RPC unreachable; skip run."); return []

    if not ws.get("initialized"):
        known |= current_mints(wallet)
        ws["seen"] = [s["signature"] for s in sigs]
        ws["known_mints"] = list(known); ws["initialized"] = True
        log(f"[{label}] Initialized wallet-wide. Seeded {len(sigs)} sigs, {len(known)} known mints. Watching forward.")
        return []

    new = [s for s in sigs if s["signature"] not in seen and not s.get("err")]
    if not new:
        for s in sigs: seen.add(s["signature"])
        ws["seen"]=list(seen); ws["known_mints"]=list(known)
        log(f"[{label}] No new transactions."); return []

    alerts = []
    for s in reversed(new):  # oldest first
        sig = s["signature"]
        tx = rpc("getTransaction", [sig, {"encoding":"jsonParsed","maxSupportedTransactionVersion":0}])
        seen.add(sig)
        if not tx: seen.discard(sig); log(f"[{label}] fetch failed {sig}; retry next run"); continue
        bt = s.get("blockTime")
        ts = datetime.datetime.fromtimestamp(bt, datetime.UTC).strftime("%Y-%m-%d %H:%M:%S UTC") if bt else "?"
        deltas = owner_deltas(tx["meta"], wallet)
        # USD spent this tx (quote side), best-effort
        usdc_spent = max(0.0, -deltas.get("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",0.0)) \
                   + max(0.0, -deltas.get("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",0.0))
        wsol_spent = max(0.0, -deltas.get("So11111111111111111111111111111111111111112",0.0))
        native_spent = max(0.0, -native_sol_delta(tx, wallet))
        # Real BUY only if it actually PAID (spent SOL/USDC/WSOL). Airdrops/transfers-in pay nothing.
        paid = (usdc_spent > 0.01) or (wsol_spent > 1e-4) or (native_spent > 0.002)
        for mint, dv in deltas.items():
            if mint in QUOTE or dv <= 1e-9: continue   # only tokens it RECEIVED
            if not paid:
                log(f"[{label}] IGNORED airdrop/transfer-in {dv:,.4f} {mint[:6]}… (no payment) at {ts} {sig}")
                continue
            sym, price = dexscreener(mint)
            usd = (dv*price) if price else 0.0
            if not usd:  # fall back to quote spent
                if usdc_spent: usd = usdc_spent
                elif wsol_spent:
                    _, solp = dexscreener("So11111111111111111111111111111111111111112")
                    usd = wsol_spent*(solp or 0)
            is_new = mint not in known
            known.add(mint)
            lbl = sym or (mint[:6]+"…")
            log(f"[{label}] BUY {dv:,.4f} {lbl} (~${usd:,.0f}) new={is_new} at {ts} {sig}")
            if ALERT_EVERY_BUY or is_new or usd >= THRESHOLD_USD:
                alerts.append({"who":label,"new":is_new,"sym":sym,"mint":mint,"amt":dv,"usd":usd,"ts":ts,"sig":sig})

        # --- SELL & MONEY-IN detection (tx-level, mutually exclusive with a BUY) ---
        _, solp = dexscreener(SOL_MINT); solp = solp or 0.0
        native_d = native_sol_delta(tx, wallet)
        wsol_d   = deltas.get(SOL_MINT, 0.0)
        usdc_d   = deltas.get(USDC_MINT, 0.0) + deltas.get(USDT_MINT, 0.0)
        token_in  = [(m,v) for m,v in deltas.items() if m not in QUOTE and v >  1e-9]
        token_out = [(m,v) for m,v in deltas.items() if m not in QUOTE and v < -1e-9]
        quote_in_usd  = max(0.0, native_d)*solp + max(0.0, wsol_d)*solp + max(0.0, usdc_d)
        quote_out_usd = max(0.0,-native_d)*solp + max(0.0,-wsol_d)*solp + max(0.0,-usdc_d)

        if ALERT_SELLS and token_out and not token_in and quote_in_usd >= MONEYIN_USD_MIN:
            # he swapped a token OUT for SOL/USDC — a sell
            for mint, dv in token_out:
                sym, price = dexscreener(mint); lbl = sym or (mint[:6]+"…")
                log(f"[{label}] SELL {-dv:,.4f} {lbl} (~${quote_in_usd:,.0f}) at {ts} {sig}")
                alerts.append({"who":label,"sell":True,"sym":sym,"mint":mint,"amt":-dv,"usd":quote_in_usd,"ts":ts,"sig":sig})

        elif ALERT_MONEY_IN and not token_in and not token_out \
             and quote_in_usd >= MONEYIN_USD_MIN and quote_out_usd <= 0.01:
            # value ARRIVED without any token being bought or sold = funding-in
            if usdc_d > 0: src = spl_sender(tx["meta"], wallet, {USDC_MINT, USDT_MINT})
            else:          src = native_sender(tx, wallet)
            exch = EXCHANGE_ADDRESSES.get(src or "")
            asset = f"{max(native_d,0.0)+max(wsol_d,0.0):,.3f} SOL" if (max(native_d,0.0)+max(wsol_d,0.0))*solp >= usdc_d else f"{usdc_d:,.2f} USDC"
            frm = exch or (f"wallet {src[:4]}…{src[-4:]}" if src else "unknown")
            log(f"[{label}] MONEY-IN {asset} (~${quote_in_usd:,.0f}) from {frm} at {ts} {sig}")
            alerts.append({"who":label,"money_in":True,"exch":exch,"src":src,"asset":asset,"usd":quote_in_usd,"ts":ts,"sig":sig})

    ws["seen"]=list(seen); ws["known_mints"]=list(known)
    if not alerts:
        log(f"[{label}] {len(new)} new txns, nothing alert-worthy (no new tokens, none ≥ ${THRESHOLD_USD:.0f}).")
    return alerts

def main():
    st = load_state()
    all_alerts = []
    for w in WALLETS:
        ws = st["wallets"][w["address"]]
        try:
            all_alerts += process_wallet(w, ws)
        except Exception as e:
            log(f"[{w['label']}] ERROR: {type(e).__name__}: {e}")
    save_state(st)

    for a in all_alerts:
        who = a["who"]
        usd = f"~${a['usd']:,.0f}" if a.get("usd") else "~$?"
        link = f"https://solscan.io/tx/{a['sig']}"
        if a.get("money_in"):                       # funds arriving
            frm = a["exch"] or (f"wallet {a['src'][:4]}…{a['src'][-4:]}" if a.get("src") else "unknown")
            kind = "EXCH-IN" if a["exch"] else "IN"
            title = f"💰 {who} received {a['asset']} ({usd}) from {frm}"
            tags  = "moneybag,inbox_tray"
            body  = f"{who} received {a['asset']} ({usd}) from {frm} at {a['ts']}.\n{link}"
            banner = f"{a['asset']} from {frm}"
        elif a.get("sell"):                          # token sold for SOL/USDC
            lbl = a["sym"] or (a["mint"][:8]+"…")
            kind = "SELL"
            title = f"🔻 {who} SOLD {lbl} — {usd}"
            tags  = "small_red_triangle_down,money_with_wings"
            body  = (f"{who} sold {a['amt']:,.0f} {lbl} for {usd} at {a['ts']}.\n"
                     f"Token: {a['mint']}\n{link}")
            banner = f"{a['amt']:,.0f} {lbl} ({usd})"
        else:                                        # buy
            lbl = a["sym"] or (a["mint"][:8]+"…")
            if a.get("new"):
                kind = "NEW"; title = f"🆕 {who} APED a NEW token: {lbl}"; tags = "seedling,rotating_light"
            else:
                kind = "BUY"; title = f"🟢 {who} bought {lbl} — {usd}"; tags = "large_green_circle,money_with_wings"
            body  = (f"{who} just bought {a['amt']:,.0f} {lbl} for {usd} at {a['ts']}.\n"
                     f"Token: {a['mint']}\n{link}")
            banner = f"{a['amt']:,.0f} {lbl} ({usd})"
        notify_phone(title, body, tags); notify_mac(title, banner)
        log(f"ALERTED [{kind}] {who}: {title}")

if __name__ == "__main__":
    try: main()
    except Exception as e:
        log(f"FATAL: {type(e).__name__}: {e}"); sys.exit(1)
