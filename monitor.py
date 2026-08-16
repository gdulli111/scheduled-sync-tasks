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
THRESHOLD_USD = 500.0
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
    try:
        urllib.request.urlopen(urllib.request.Request(
            "https://ntfy.sh/"+TOPIC, data=message.encode("utf-8"),
            headers={"Title": title, "Priority": "high", "Tags": tags, "User-Agent": UA}), timeout=20)
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
            if is_new or usd >= THRESHOLD_USD:
                alerts.append({"who":label,"new":is_new,"sym":sym,"mint":mint,"amt":dv,"usd":usd,"ts":ts,"sig":sig})

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
        label = a["sym"] or (a["mint"][:8]+"…")
        usd = f"~${a['usd']:,.0f}" if a["usd"] else "~$?"
        if a["new"]:
            title = f"🆕 {who} APED a NEW token: {label}"
            tags  = "seedling,rotating_light"
        else:
            title = f"{who} bought {label} — {usd}"
            tags  = "rotating_light,money_with_wings"
        body = (f"{who} just bought {a['amt']:,.0f} {label} for {usd} at {a['ts']}.\n"
                f"Token: {a['mint']}\nhttps://solscan.io/tx/{a['sig']}")
        notify_phone(title, body, tags); notify_mac(title, f"{a['amt']:,.0f} {label} ({usd})")
        log(f"ALERTED [{'NEW' if a['new'] else 'BIG'}] {who}: {title}")

if __name__ == "__main__":
    try: main()
    except Exception as e:
        log(f"FATAL: {type(e).__name__}: {e}"); sys.exit(1)
