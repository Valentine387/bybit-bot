#!/usr/bin/env python3
"""
James Algorithm — Bybit Proxy Server
Handles: Bybit REST API (Spot + Futures), WebSocket price feeds, Claude AI
Run: python3 bybit_proxy.py
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import urllib.request, urllib.error, urllib.parse
import ssl, json, hmac, hashlib, time, threading
import websocket  # pip install websocket-client

import os

PORT       = int(os.environ.get("PORT", 8766))   # Render sets PORT automatically
BYBIT_LIVE = "https://api.bybit.com"
BYBIT_DEMO = "https://api-demo.bybit.com"
CLAUDE_URL = "https://api.anthropic.com/v1/messages"
ALPACA_NEWS = "https://data.alpaca.markets/v1beta1"

# ── Set via Render environment variable DEMO_MODE=true/false ──────────
USE_DEMO = os.environ.get("DEMO_MODE", "true").lower() == "true"
BASE_URL  = BYBIT_DEMO if USE_DEMO else BYBIT_LIVE

# ── Per-request demo/live routing ─────────────────────────────────────
# Each browser request may carry ?demo=true|false. Background loops set
# their thread context from the stored credentials. Fallback: env default.
import threading as _threading_mod
_req_ctx = _threading_mod.local()

def _base(demo=None):
    """Effective Bybit base URL for this request/thread."""
    if demo is None:
        demo = getattr(_req_ctx, 'demo', None)
    if demo is None:
        demo = USE_DEMO
    return BYBIT_DEMO if demo else BYBIT_LIVE

def _parse_demo_param(path_qs):
    """Extract demo flag from a query string; None if absent."""
    try:
        qs = urllib.parse.urlparse(path_qs).query
        v = urllib.parse.parse_qs(qs).get('demo', [None])[0]
        if v is None: return None
        return str(v).lower() == 'true'
    except Exception:
        return None

# ── Windows DNS fix ────────────────────────────────────────────────────
# Windows sometimes blocks Python DNS — patch to use Google 8.8.8.8
import socket as _socket, subprocess as _subprocess, re as _re
_orig_getaddrinfo = _socket.getaddrinfo
def _patched_getaddrinfo(host, port, *args, **kwargs):
    try:
        return _orig_getaddrinfo(host, port, *args, **kwargs)
    except _socket.gaierror:
        try:
            out = _subprocess.run(['nslookup', host, '8.8.8.8'],
                capture_output=True, text=True, timeout=5).stdout
            ips = [ip for ip in _re.findall(r'Address:\s*(\d+\.\d+\.\d+\.\d+)', out)
                   if not ip.startswith('8.8')]
            if ips:
                return _orig_getaddrinfo(ips[0], port, *args, **kwargs)
        except Exception:
            pass
        raise
_socket.getaddrinfo = _patched_getaddrinfo

ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE

# ── WebSocket price cache ──────────────────────────────────────────────
# Stores latest prices pushed by Bybit WebSocket
price_cache = {}   # symbol → { price, change24h, timestamp }
ws_subscribed = set()
ws_app = None
ws_thread = None
api_key_global = ""
api_secret_global = ""

def safe_float(val, default=0.0):
    """Safely convert any value to float — handles empty strings, None, etc."""
    try:
        return float(val) if val not in (None, '', 'None') else default
    except (ValueError, TypeError):
        return default

# ── Bybit server time sync ─────────────────────────────────────────────
# Windows clocks often drift — sync with Bybit server time to avoid 10002
_time_offset_ms = 0  # milliseconds to add to local time

def sync_bybit_time():
    """Fetch Bybit server time and calculate offset from local clock"""
    global _time_offset_ms
    try:
        url = "https://api.bybit.com/v5/market/time"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, context=ssl_ctx, timeout=5) as r:
            d = json.loads(r.read())
            server_ms  = int(d.get("result", {}).get("timeNano", "0")[:13] or
                            d.get("result", {}).get("timeSecond", "0") + "000")
            local_ms   = int(time.time() * 1000)
            _time_offset_ms = server_ms - local_ms
            print(f"    TIME SYNC: offset={_time_offset_ms}ms (local {'fast' if _time_offset_ms < 0 else 'slow'} by {abs(_time_offset_ms)}ms)")
    except Exception as e:
        print(f"    TIME SYNC failed: {e} — using local time")
        _time_offset_ms = 0

def bybit_timestamp():
    """Return current timestamp in ms, corrected for server time offset"""
    return int(time.time() * 1000) + _time_offset_ms

def bybit_sign(api_key, api_secret, params_str):
    """
    Bybit V5 API signature:
    sign_str = timestamp + api_key + recv_window + params_str
    recv_window increased to 20000ms to handle clock drift
    """
    timestamp   = str(bybit_timestamp())
    recv_window = "20000"   # 20s window — handles up to 20s clock drift
    sign_str    = timestamp + api_key + recv_window + params_str
    signature   = hmac.new(
        api_secret.encode('utf-8'),
        sign_str.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    return timestamp, recv_window, signature

def bybit_headers(api_key, api_secret, params_str=""):
    timestamp, recv_window, signature = bybit_sign(api_key, api_secret, params_str)
    return {
        "X-BAPI-API-KEY":     api_key,
        "X-BAPI-TIMESTAMP":   timestamp,
        "X-BAPI-RECV-WINDOW": recv_window,
        "X-BAPI-SIGN":        signature,
        "Content-Type":       "application/json",
    }

def bybit_get(path, params, api_key, api_secret):
    sorted_params = dict(sorted(params.items()))
    query = urllib.parse.urlencode(sorted_params)
    url   = _base() + path + ("?" + query if query else "")
    hdrs  = bybit_headers(api_key, api_secret, query)
    req   = urllib.request.Request(url)
    for k, v in hdrs.items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, context=ssl_ctx, timeout=10) as r:
            raw = r.read()
            d = json.loads(raw)
            ret_code = d.get("retCode", 0)
            ret_msg  = d.get("retMsg", "")
            print(f"    bybit_get {path} → retCode={ret_code} retMsg={ret_msg}")
            if ret_code != 0:
                raise Exception(f"Bybit retCode {ret_code}: {ret_msg}")
            return d
    except urllib.error.HTTPError as e:
        raw = e.read()
        print(f"    bybit_get HTTP {e.code}: {raw[:200]}")
        try:
            err = json.loads(raw)
            raise Exception(f"Bybit HTTP {e.code}: {err.get('retMsg', err)}")
        except json.JSONDecodeError:
            raise Exception(f"Bybit HTTP {e.code}: {raw[:100]}")

def bybit_post(path, body, api_key, api_secret):
    body_str = json.dumps(body, separators=(',', ':'))  # compact JSON
    hdrs     = bybit_headers(api_key, api_secret, body_str)
    url      = _base() + path
    req      = urllib.request.Request(url, data=body_str.encode('utf-8'), method="POST")
    for k, v in hdrs.items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, context=ssl_ctx, timeout=10) as r:
            d = json.loads(r.read())
            if d.get("retCode", 0) != 0:
                raise Exception(d.get("retMsg", "Bybit API error"))
            return d
    except urllib.error.HTTPError as e:
        err = json.loads(e.read())
        raise Exception(err.get("retMsg", str(e)))

# ── WebSocket price feed ───────────────────────────────────────────────
def start_ws(symbols):
    """Subscribe to real-time ticker updates for given symbols"""
    global ws_app, ws_thread

    # Bybit demo and live use same WebSocket stream
    ws_url = "wss://stream.bybit.com/v5/public/spot"

    topics = [f"tickers.{s}" for s in symbols]

    def on_message(ws, message):
        try:
            d = json.loads(message)
            if d.get("topic","").startswith("tickers."):
                data = d.get("data", {})
                sym  = data.get("symbol","")
                if sym:
                    price_cache[sym] = {
                        "price":     safe_float(data.get("lastPrice", 0)),
                        "change24h": safe_float(data.get("price24hPcnt", 0)) * 100,
                        "bid":       safe_float(data.get("bid1Price", 0)),
                        "ask":       safe_float(data.get("ask1Price", 0)),
                        "volume":    safe_float(data.get("volume24h", 0)),
                        "ts":        time.time(),
                    }
        except Exception as e:
            print(f"WS parse error: {e}")

    def on_open(ws):
        sub = {"op": "subscribe", "args": topics}
        ws.send(json.dumps(sub))
        print(f"WS subscribed: {topics}")

    def on_error(ws, error):
        print(f"WS error: {error}")

    def on_close(ws, *args):
        print("WS closed")

    ws_app = websocket.WebSocketApp(
        ws_url,
        on_message=on_message,
        on_open=on_open,
        on_error=on_error,
        on_close=on_close,
    )
    ws_thread = threading.Thread(target=ws_app.run_forever, daemon=True)
    ws_thread.start()

def subscribe_symbol(symbol):
    """Add a new symbol to WebSocket subscription"""
    global ws_app
    if symbol in ws_subscribed:
        return
    ws_subscribed.add(symbol)
    if ws_app:
        sub = {"op": "subscribe", "args": [f"tickers.{symbol}"]}
        try:
            ws_app.send(json.dumps(sub))
        except:
            pass

# ── HTTP Proxy Handler ─────────────────────────────────────────────────
class BybitProxyHandler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        pass  # suppress default logs

    def send_cors(self):
        # Allow all origins including VS Code Live Server (127.0.0.1:5500)
        origin = self.headers.get('Origin', '*')
        self.send_header("Access-Control-Allow-Origin", origin)
        self.send_header("Access-Control-Allow-Credentials", "true")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Access-Control-Max-Age", "86400")

    def send_json(self, status, data):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_cors()
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_cors()
        self.end_headers()

    def do_GET(self):
        _req_ctx.demo = _parse_demo_param(self.path)
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        api_key    = params.get("key",    [""])[0]
        api_secret = params.get("secret", [""])[0]

        path = parsed.path

        if path == "/price":
            self.handle_price(params)
        elif path == "/account":
            self.handle_account(api_key, api_secret)
        elif path == "/positions":
            category = params.get("category", ["linear"])[0]
            self.handle_positions(api_key, api_secret, category)
        elif path == "/balance":
            self.handle_balance(api_key, api_secret)
        elif path == "/news":
            self.handle_news(params)
        elif path == "/klines" or path == "/kline":
            self.handle_kline(params)
        elif path == "/instrument":
            self.handle_instrument(params)
        elif path == "/order-history":
            # Fetch closed order history from Bybit
            # Supports: ?hours=24&category=linear&limit=50
            api_key    = params.get("key",      [""])[0]
            api_secret = params.get("secret",   [""])[0]
            category   = params.get("category", ["linear"])[0]
            limit      = int(params.get("limit", ["100"])[0])
            hours      = int(params.get("hours", ["24"])[0])

            try:
                import time as _time
                # Calculate start time in ms
                start_ms = int((_time.time() - hours * 3600) * 1000)

                all_orders = []
                for cat in ([category] if category != 'all' else ['linear','spot']):
                    try:
                        d = bybit_get("/v5/order/history", {
                            "category":   cat,
                            "limit":      str(limit),
                            "startTime":  str(start_ms),
                            "orderStatus": "Filled",
                        }, api_key, api_secret)
                        orders = d.get("result", {}).get("list", [])
                        for o in orders:
                            # Normalise to our format
                            qty       = float(o.get("qty", 0))
                            avg_price = float(o.get("avgPrice", 0) or o.get("price", 0))
                            cum_exec  = float(o.get("cumExecValue", 0))
                            cum_fee   = float(o.get("cumExecFee", 0))
                            side      = o.get("side", "Buy")
                            symbol    = o.get("symbol", "")
                            created   = int(o.get("createdTime", 0))
                            updated   = int(o.get("updatedTime", created))

                            # Try to get P&L from closed PnL endpoint for futures
                            all_orders.append({
                                "orderId":    o.get("orderId", ""),
                                "symbol":     symbol,
                                "side":       side,
                                "qty":        qty,
                                "avg_price":  avg_price,
                                "value":      cum_exec,
                                "fee":        cum_fee,
                                "category":   cat,
                                "created_ms": created,
                                "updated_ms": updated,
                                "status":     o.get("orderStatus", "Filled"),
                            })
                    except Exception as e:
                        print(f"  order-history {cat}: {e}")

                # Also fetch closed PnL for futures (has actual P&L data)
                pnl_map = {}
                try:
                    pnl_d = bybit_get("/v5/position/closed-pnl", {
                        "category":  "linear",
                        "limit":     str(limit),
                        "startTime": str(start_ms),
                    }, api_key, api_secret)
                    for p in pnl_d.get("result", {}).get("list", []):
                        sym = p.get("symbol", "")
                        pnl_map[sym] = pnl_map.get(sym, [])
                        pnl_map[sym].append({
                            "pnl":        float(p.get("closedPnl", 0)),
                            "entry":      float(p.get("avgEntryPrice", 0)),
                            "exit":       float(p.get("avgExitPrice", 0)),
                            "qty":        float(p.get("qty", 0)),
                            "created_ms": int(p.get("createdTime", 0)),
                            "updated_ms": int(p.get("updatedTime", 0)),
                            "side":       p.get("side", "Buy"),
                        })
                except Exception as e:
                    print(f"  closed-pnl: {e}")

                # Sort by time descending
                all_orders.sort(key=lambda x: x["updated_ms"], reverse=True)

                self.send_json(200, {
                    "orders": all_orders,
                    "pnl":    pnl_map,
                    "count":  len(all_orders),
                    "hours":  hours,
                })
            except Exception as e:
                self.send_json(500, {"error": str(e)})

        elif path == "/ping" or path == "/healthz":
            # Simple connectivity test — Render uses /healthz for health checks
            self.send_json(200, {
                "status": "proxy_ok", "url": BASE_URL, "demo": USE_DEMO,
                "auto_enabled": _auto_creds['auto_enabled'],
                "regime": _last_regime,
                "open_positions": len(_open_positions),
            })

        elif path == "/set-credentials":
            # Browser pushes API keys + settings to server for autonomous trading
            body = json.loads(self.rfile.read(int(self.headers.get('Content-Length',0))))
            # Only update keys if provided and non-empty
            if body.get('api_key'):    _auto_creds['api_key']    = body['api_key']
            if body.get('api_secret'): _auto_creds['api_secret'] = body['api_secret']
            if body.get('trading_mode'): _auto_creds['trading_mode'] = body['trading_mode']
            if body.get('trade_size'):   _auto_creds['trade_size']   = float(body['trade_size'])
            if body.get('tp_pct'):       _auto_creds['tp_pct']       = float(body['tp_pct'])
            if body.get('sl_pct'):       _auto_creds['sl_pct']       = float(body['sl_pct'])
            if body.get('min_confidence'): _auto_creds['min_confidence'] = float(body['min_confidence'])
            if 'demo' in body: _auto_creds['demo'] = bool(body['demo']) if not isinstance(body['demo'], str) else body['demo'].lower() == 'true'
            # auto_enabled: only update if explicitly included in request
            if 'auto_enabled' in body:
                _auto_creds['auto_enabled'] = bool(body['auto_enabled'])
                # Persist user choice so Render restarts don't override it
                try:
                    with open('/tmp/auto_trading_state.txt', 'w') as _f:
                        _f.write('true' if _auto_creds['auto_enabled'] else 'false')
                except Exception: pass
            enabled = _auto_creds['auto_enabled']
            print(f"  [AutoTrader] Credentials updated — auto={'ON ✅' if enabled else 'OFF'} mode={_auto_creds['trading_mode']} size=${_auto_creds['trade_size']}")
            self.send_json(200, {"status": "ok", "auto_enabled": enabled})

        elif path == "/auto-status":
            # Get current autonomous trading status
            self.send_json(200, {
                "auto_enabled":   _auto_creds['auto_enabled'],
                "has_credentials": bool(_auto_creds['api_key']),
                "regime":         _last_regime,
                "open_positions": len(_open_positions),
                "trading_mode":   _auto_creds['trading_mode'],
                "trade_size":     _auto_creds['trade_size'],
                "min_confidence": _auto_creds['min_confidence'],
                "auto_env_default": os.environ.get('AUTO_TRADING', 'false'),
                "telegram_enabled": _telegram_enabled(),
                "build":          "v3.6-stripe",
                "demo":           _auto_creds.get('demo', USE_DEMO),
            })

        elif path == "/check-subscription":
            email = params.get("email", [""])[0]
            if not STRIPE_SECRET_KEY:
                self.send_json(200, {"active": False, "error": "stripe not configured yet"})
            elif not email or "@" not in email:
                self.send_json(400, {"active": False, "error": "valid email required"})
            else:
                self.send_json(200, _check_stripe_subscription(email))

        elif path == "/site-news":
            # Website news feed (server-side Alpaca keys, cached 5 min)
            self.send_json(200, _fetch_alpaca_news(10))

        elif path == "/telegram-test":
            # Diagnostic: verify Telegram config end-to-end from any browser
            if not TELEGRAM_BOT_TOKEN:
                self.send_json(200, {"ok": False, "error": "TELEGRAM_BOT_TOKEN env var is not set on Render"})
            elif not TELEGRAM_CHAT_ID:
                self.send_json(200, {"ok": False, "error": "TELEGRAM_CHAT_ID env var is not set on Render"})
            else:
                ok, detail = _telegram_send_detailed(
                    "\u2705 <b>Test message</b> \u2014 your TradeAlgorythm server can post to this channel. "
                    "Strong signals (60%+) will appear here automatically.")
                self.send_json(200, {
                    "ok": ok,
                    "detail": detail,
                    "chat_id_configured": TELEGRAM_CHAT_ID[:4] + "..." if len(TELEGRAM_CHAT_ID) > 4 else TELEGRAM_CHAT_ID,
                })

        elif path == "/auto-toggle":
            # Toggle auto trading on/off and persist the choice
            _auto_creds['auto_enabled'] = not _auto_creds['auto_enabled']
            state = 'ON ✅' if _auto_creds['auto_enabled'] else 'OFF'
            has_creds = bool(_auto_creds.get('api_key')) and bool(_auto_creds.get('api_secret'))
            print(f"  [AutoTrader] Auto-trading toggled → {state} | has_credentials={has_creds}")
            # Persist so Render restart does not override user choice
            try:
                with open('/tmp/auto_trading_state.txt', 'w') as _f:
                    _f.write('true' if _auto_creds['auto_enabled'] else 'false')
            except Exception: pass
            if _auto_creds['auto_enabled'] and not has_creds:
                print(f"  [AutoTrader] ⚠ No credentials — set BYBIT_API_KEY env var or connect from browser first")
            self.send_json(200, {
                "auto_enabled": _auto_creds['auto_enabled'],
                "status": state,
                "has_credentials": has_creds,
            })
        elif path == "/test":
            # Test API keys with simplest possible Bybit call
            self.handle_test(api_key, api_secret)
        elif path == "/ws_status":
            self.send_json(200, {"subscribed": list(ws_subscribed), "cache": list(price_cache.keys())})
        else:
            self.send_json(404, {"error": "Not found"})

    def do_POST(self):
        _req_ctx.demo = _parse_demo_param(self.path)
        parsed = urllib.parse.urlparse(self.path)
        length = int(self.headers.get("Content-Length", 0))
        body   = json.loads(self.rfile.read(length)) if length else {}
        params = urllib.parse.parse_qs(parsed.query)
        api_key    = params.get("key",    [""])[0] or body.get("key", "")
        api_secret = params.get("secret", [""])[0] or body.get("secret", "")

        path = parsed.path

        if path == "/order":
            self.handle_order(api_key, api_secret, body)
        elif path == "/close":
            self.handle_close(api_key, api_secret, body)
        elif path == "/margin-mode":
            self.handle_margin_mode(api_key, api_secret, body)
        elif path == "/subscribe":
            symbol = body.get("symbol","")
            if symbol:
                subscribe_symbol(symbol)
            self.send_json(200, {"subscribed": symbol})
        elif path == "/claude":
            self.handle_claude(api_key, api_secret, body)
        else:
            self.send_json(404, {"error": "Not found"})

    def handle_price(self, params):
        symbol   = params.get("symbol", [""])[0].upper()
        category = params.get("category", ["spot"])[0]

        # Check WebSocket cache first (real-time)
        if symbol in price_cache:
            cached = price_cache[symbol]
            age = time.time() - cached.get("ts", 0)
            if age < 10:  # use cache if less than 10s old
                self.send_json(200, {"symbol": symbol, "source": "websocket", **cached})
                return

        # Fallback to REST API
        try:
            d = bybit_get("/v5/market/tickers",
                {"category": category, "symbol": symbol},
                "", "")  # public endpoint, no auth needed
            items = d.get("result", {}).get("list", [])
            if not items:
                self.send_json(404, {"error": f"Symbol {symbol} not found"})
                return
            item = items[0]
            result = {
                "symbol":    symbol,
                "price":     safe_float(item.get("lastPrice", 0)),
                "change24h": safe_float(item.get("price24hPcnt", 0)) * 100,
                "bid":       safe_float(item.get("bid1Price", 0)),
                "ask":       safe_float(item.get("ask1Price", 0)),
                "volume":    safe_float(item.get("volume24h", 0)),
                "source":    "rest",
            }
            # Cache it
            price_cache[symbol] = {**result, "ts": time.time()}
            self.send_json(200, result)
        except Exception as e:
            self.send_json(500, {"error": str(e)})

    def handle_account(self, api_key, api_secret):
        print(f"\n--- BYBIT ACCOUNT (key={api_key[:8] if api_key else 'EMPTY'}...)")
        if not api_key or not api_secret:
            self.send_json(400, {"error": "API key and secret are required"})
            return
        try:
            # Bybit V5 wallet balance — try all account types
            last_error = None
            for account_type in ["UNIFIED", "SPOT", "CONTRACT"]:
                try:
                    d = bybit_get("/v5/account/wallet-balance",
                        {"accountType": account_type}, api_key, api_secret)
                    coins = d.get("result", {}).get("list", [{}])[0].get("coin", [])
                    usdt  = next((c for c in coins if c.get("coin") == "USDT"), None)
                    print(f"    {account_type}: coins={[c.get('coin') for c in coins]}")
                    if usdt:
                        result = {
                            "equity":            safe_float(usdt.get("equity") or usdt.get("walletBalance", 0)),
                            "available_balance": safe_float(usdt.get("availableToWithdraw") or usdt.get("available") or usdt.get("walletBalance", 0)),
                            "wallet_balance":    safe_float(usdt.get("walletBalance", 0)),
                            "unrealized_pnl":    safe_float(usdt.get("unrealisedPnl", 0)),
                            "account_type":      account_type,
                        }
                        print(f"    SUCCESS: equity={result['equity']}")
                        self.send_json(200, result)
                        return
                except Exception as e:
                    last_error = str(e)
                    print(f"    {account_type} failed: {e}")
                    continue

            # All account types failed — give helpful error
            err = last_error or "Could not access Bybit account"
            # Parse common Bybit error codes for clearer messages
            if "10003" in str(err) or "invalid api_key" in str(err).lower():
                err = "Invalid API key — check you copied the full key correctly (no spaces)"
            elif "10004" in str(err) or "sign" in str(err).lower():
                err = "Invalid API signature — check your Secret Key is correct"
            elif "10005" in str(err) or "permission" in str(err).lower():
                err = "API key missing Trade permission — enable it on Bybit API Management page"
            elif "33004" in str(err) or "ip" in str(err).lower():
                err = "IP not whitelisted — remove IP restriction from your Bybit API key settings"
            elif "getaddrinfo" in str(err).lower() or "11001" in str(err):
                err = "Cannot reach Bybit — check internet connection (DNS error)"
            raise Exception(err)

        except Exception as e:
            print(f"    ACCOUNT FINAL ERROR: {e}")
            self.send_json(400, {"error": str(e)})

    def handle_positions(self, api_key, api_secret, category):
        positions = []  # always initialise FIRST — prevents UnboundLocalError
        try:
            # Bybit /v5/position/list only supports linear and option — NOT spot
            if category == 'spot':
                # Spot holdings live in wallet balance, not position list
                try:
                    d = bybit_get("/v5/account/wallet-balance",
                        {"accountType": "UNIFIED"}, api_key, api_secret)
                    coins = d.get("result", {}).get("list", [{}])[0].get("coin", [])
                    for c in coins:
                        coin = c.get("coin", "")
                        if coin in ("USDT","USDC","USD","USDE"): continue
                        qty = safe_float(c.get("walletBalance", 0))
                        if qty <= 0: continue
                        avg_price = safe_float(c.get("avgPrice", 0))
                        unreal    = safe_float(c.get("unrealisedPnl", 0))
                        # Fetch current market price for spot positions
                        cur_price = avg_price
                        try:
                            tick = bybit_get("/v5/market/tickers",
                                {"category":"spot","symbol":coin+"USDT"})
                            tick_list = tick.get("result",{}).get("list",[])
                            if tick_list:
                                cur_price = safe_float(tick_list[0].get("lastPrice", avg_price))
                        except: pass
                        cost   = avg_price * qty
                        pl_pct = ((cur_price - avg_price) / avg_price) if avg_price > 0 else 0
                        unreal = (cur_price - avg_price) * qty
                        positions.append({
                            "symbol":          coin + "USDT",
                            "side":            "Buy",
                            "size":            qty,
                            "qty":             qty,
                            "avg_entry_price": avg_price,
                            "current_price":   cur_price,
                            "unrealized_pl":   unreal,
                            "unrealized_plpc": pl_pct,  # decimal e.g. 0.05 = 5%
                            "cost_basis":      cost,
                            "leverage":        1,
                            "category":        "spot",
                            "positionIdx":     0,
                        })
                except Exception as e:
                    print(f"    SPOT BALANCE ERROR: {e}")
                self.send_json(200, positions)
                return

            # linear or option — use position list, paginate to get all positions
            all_pos_list = []
            cursor = None
            while True:
                params = {"category": category, "settleCoin": "USDT", "limit": "200"}
                if cursor:
                    params["cursor"] = cursor
                d = bybit_get("/v5/position/list", params, api_key, api_secret)
                page = d.get("result", {}).get("list", [])
                all_pos_list.extend(page)
                cursor = d.get("result", {}).get("nextPageCursor", "")
                if not cursor or len(page) < 200:
                    break
            for p in all_pos_list:
                size = safe_float(p.get("size", 0))
                if size == 0: continue
                entry  = safe_float(p.get("avgPrice", 0))
                mark   = safe_float(p.get("markPrice", 0))
                side   = p.get("side", "Buy")
                unreal = safe_float(p.get("unrealisedPnl", 0))
                lev    = safe_float(p.get("leverage", 1)) or 1
                cost   = entry * size / lev if lev > 0 else entry * size
                pl_pct = (unreal / cost * 100) if cost > 0 else 0
                positions.append({
                    "symbol":          p.get("symbol"),
                    "side":            side,
                    "size":            size,
                    "qty":             size,
                    "avg_entry_price": entry,
                    "current_price":   mark,
                    "unrealized_pl":   unreal,
                    "unrealized_plpc": pl_pct / 100,
                    "cost_basis":      cost,
                    "leverage":        lev,
                    "category":        category,
                    "positionIdx":     p.get("positionIdx", 0),
                })
        except Exception as e:
            print(f"    POSITIONS ERROR: {e}")
        # Always return whatever we have — even if empty
        self.send_json(200, positions)

    def handle_margin_mode(self, api_key, api_secret, body):
        """Set margin mode for a futures position — isolated or cross"""
        symbol    = body.get("symbol", "")
        category  = body.get("category", "linear")
        trade_mode = body.get("tradeMode", 1)  # 0=cross, 1=isolated
        print(f"\n--- MARGIN MODE {symbol} mode={trade_mode}")
        try:
            d = bybit_post("/v5/position/switch-isolated", {
                "symbol":    symbol,
                "category":  category,
                "tradeMode": trade_mode,
                "buyLeverage": str(body.get("leverage", "1")),
                "sellLeverage": str(body.get("leverage", "1")),
            }, api_key, api_secret)
            print(f"    MARGIN MODE: {d.get('retCode')} {d.get('retMsg')}")
            self.send_json(200, {"ok": True, "msg": d.get("retMsg","")})
        except Exception as e:
            print(f"    MARGIN MODE ERROR: {e}")
            self.send_json(200, {"ok": False, "msg": str(e)})  # non-fatal

    def handle_order(self, api_key, api_secret, body):
        """Place spot or futures order"""
        category = body.get("category", "spot")
        symbol   = body.get("symbol", "")
        side     = body.get("side", "Buy")   # Buy or Sell
        qty      = body.get("qty", "")       # base quantity for spot
        notional = body.get("notional", "")  # USD value (for spot market buy)
        order_type = body.get("orderType", "Market")
        leverage = body.get("leverage", None)

        print(f"\n--- BYBIT ORDER {side} {symbol} cat={category} qty={qty} notional={notional}")

        try:
            # Set leverage for futures before ordering
            if category == "linear" and leverage:
                try:
                    bybit_post("/v5/position/set-leverage", {
                        "category":    "linear",
                        "symbol":      symbol,
                        "buyLeverage": str(leverage),
                        "sellLeverage": str(leverage),
                    }, api_key, api_secret)
                except:
                    pass  # leverage may already be set

            order_body = {
                "category":  category,
                "symbol":    symbol,
                "side":      side,
                "orderType": order_type,
                "timeInForce": "IOC" if order_type == "Market" else "GTC",
            }

            if notional and category == "spot" and side == "Buy":
                # Market buy by USD value (e.g. spend $100 on BTC)
                order_body["marketUnit"] = "quoteCoin"
                order_body["qty"] = str(notional)
            elif qty:
                order_body["qty"] = str(qty)

            result = bybit_post("/v5/order/create", order_body, api_key, api_secret)
            print(f"    ORDER RESULT: {result.get('retCode')} {result.get('retMsg')}")
            if result.get("retCode") != 0:
                self.send_json(400, {"error": result.get("retMsg", "Order failed")})
            else:
                self.send_json(200, result.get("result", {}))
        except Exception as e:
            print(f"    ORDER ERROR: {e}")
            self.send_json(500, {"error": str(e)})

    def handle_close(self, api_key, api_secret, body):
        """Close a position — spot sell all or futures close"""
        category = body.get("category", "spot")
        symbol   = body.get("symbol", "")
        qty      = body.get("qty", "")
        side     = body.get("side", "Sell")  # opposite of open side
        pos_idx  = body.get("positionIdx", 0)

        print(f"\n--- BYBIT CLOSE {symbol} cat={category} qty={qty}")

        try:
            close_body = {
                "category":    category,
                "symbol":      symbol,
                "side":        side,
                "orderType":   "Market",
                "qty":         str(qty),
                "timeInForce": "IOC",
            }
            if category == "linear":
                close_body["reduceOnly"]   = True
                close_body["positionIdx"] = pos_idx

            result = bybit_post("/v5/order/create", close_body, api_key, api_secret)
            print(f"    CLOSE RESULT: {result.get('retCode')} {result.get('retMsg')}")
            if result.get("retCode") != 0:
                self.send_json(400, {"error": result.get("retMsg", "Close failed")})
            else:
                self.send_json(200, result.get("result", {}))
        except Exception as e:
            self.send_json(500, {"error": str(e)})

    def handle_instrument(self, params):
        """Get instrument info including min qty and step size — public endpoint"""
        symbol   = params.get('symbol',   ['BTCUSDT'])[0].upper()
        category = params.get('category', ['linear'])[0]
        print(f"\n--- INSTRUMENT {symbol} cat={category}")
        try:
            url = f"{BASE_URL}/v5/market/instruments-info?category={category}&symbol={symbol}"
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, context=ssl_ctx, timeout=10) as r:
                d = json.loads(r.read())
            items = d.get("result", {}).get("list", [])
            if not items:
                self.send_json(404, {"error": f"Symbol {symbol} not found"})
                return
            item = items[0]
            lot = item.get("lotSizeFilter", {})
            price_filter = item.get("priceFilter", {})
            result = {
                "symbol":    symbol,
                "category":  category,
                "minQty":    safe_float(lot.get("minOrderQty", "0.001")),
                "maxQty":    safe_float(lot.get("maxOrderQty", "9999999")),
                "qtyStep":   safe_float(lot.get("qtyStep", "0.001")),
                "minNotional": safe_float(lot.get("minNotionalValue", "1")),
                "tickSize":  safe_float(price_filter.get("tickSize", "0.01")),
            }
            print(f"    INSTRUMENT: minQty={result['minQty']} qtyStep={result['qtyStep']} minNotional={result['minNotional']}")
            self.send_json(200, result)
        except Exception as e:
            print(f"    INSTRUMENT ERROR: {e}")
            self.send_json(500, {"error": str(e)})

    def handle_kline(self, params):
        """Fetch OHLCV candle data — uses PUBLIC Bybit endpoint, no auth needed"""
        symbol   = params.get('symbol',   ['BTCUSDT'])[0].upper()
        interval = params.get('interval', ['5'])[0]
        limit    = params.get('limit',    ['200'])[0]
        category = params.get('category', ['spot'])[0]
        print(f"\n--- KLINE {symbol} {interval} limit={limit}")
        try:
            # Public endpoint — no signature needed
            url = f"{BASE_URL}/v5/market/kline?category={category}&symbol={symbol}&interval={interval}&limit={limit}"
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, context=ssl_ctx, timeout=10) as r:
                d = json.loads(r.read())
            if d.get("retCode", 0) != 0:
                raise Exception(d.get("retMsg", "Kline error"))
            raw = d.get("result", {}).get("list", [])
            raw = list(reversed(raw))  # Bybit returns newest first
            candles, volumes = [], []
            for c in raw:
                ts    = int(c[0]) // 1000
                open_ = safe_float(c[1])
                high  = safe_float(c[2])
                low   = safe_float(c[3])
                close = safe_float(c[4])
                vol   = safe_float(c[5])
                if open_ <= 0 or close <= 0:
                    continue
                candles.append({"time": ts, "open": open_, "high": high, "low": low, "close": close})
                volumes.append({"time": ts, "value": vol,
                                "color": "rgba(0,229,160,0.3)" if close >= open_ else "rgba(255,61,107,0.3)"})
            print(f"    KLINE: {len(candles)} candles OK")
            self.send_json(200, {"candles": candles, "volumes": volumes, "symbol": symbol, "interval": interval})
        except Exception as e:
            print(f"    KLINE ERROR: {e}")
            self.send_json(500, {"error": str(e)})

    def handle_test(self, api_key, api_secret):
        """Test API keys with multiple endpoints"""
        results = {}
        print(f"\n--- API KEY TEST (key={api_key[:8] if api_key else 'EMPTY'}...)")
        try:
            req = urllib.request.Request(f"{BASE_URL}/v5/market/time")
            with urllib.request.urlopen(req, context=ssl_ctx, timeout=5) as r:
                d = json.loads(r.read())
                results["server_time"] = d.get("result", {}).get("timeSecond", "ok")
                print(f"    Public API: OK")
        except Exception as e:
            results["server_time"] = f"FAIL: {e}"
        for acct_type in ["UNIFIED", "SPOT"]:
            try:
                bybit_get("/v5/account/wallet-balance", {"accountType": acct_type}, api_key, api_secret)
                results[f"auth_{acct_type}"] = "OK"
                print(f"    Auth {acct_type}: OK")
            except Exception as e:
                results[f"auth_{acct_type}"] = str(e)
                print(f"    Auth {acct_type}: FAIL — {e}")
        self.send_json(200, results)

    def handle_news(self, params):
        """Fetch news from Alpaca data API — requires Alpaca keys, not Bybit keys"""
        api_key    = params.get('key',     [''])[0]
        secret_key = params.get('secret',  [''])[0]
        symbols    = params.get('symbols', [''])[0]
        limit      = params.get('limit',   ['10'])[0]
        print(f"\n--- NEWS symbols={symbols}")
        # If keys look like Bybit keys (not Alpaca format), skip silently
        if not api_key.startswith('PK') and not api_key.startswith('AK'):
            print(f"    NEWS: Skipping — Bybit keys don't work with Alpaca news API")
            self.send_json(200, [])
            return
        url = f"{ALPACA_NEWS}/news?symbols={symbols}&limit={limit}&sort=desc"
        req = urllib.request.Request(url)
        req.add_header("APCA-API-KEY-ID", api_key)
        req.add_header("APCA-API-SECRET-KEY", secret_key)
        try:
            with urllib.request.urlopen(req, context=ssl_ctx, timeout=10) as resp:
                data = resp.read()
                print(f"    NEWS: {resp.status} OK")
                self.send_response(200)
                self.send_cors()
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(data)
        except Exception as e:
            print(f"    NEWS ERROR: {e} — returning empty")
            self.send_response(200)
            self.send_cors()
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps([]).encode())

    def handle_claude(self, api_key, api_secret, body):
        claude_key = self.headers.get("X-API-Key", "")
        print(f"\n--- CLAUDE request (key len={len(claude_key)})")
        try:
            data = json.dumps(body).encode()
            req  = urllib.request.Request(CLAUDE_URL, data=data, method="POST")
            req.add_header("Content-Type", "application/json")
            req.add_header("x-api-key", claude_key)
            req.add_header("anthropic-version", "2023-06-01")
            with urllib.request.urlopen(req, context=ssl_ctx, timeout=30) as r:
                result = r.read()
                print(f"    CLAUDE: {r.status} OK")
                self.send_response(200)
                self.send_cors()
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(result)
        except urllib.error.HTTPError as e:
            data = e.read()
            print(f"    CLAUDE ERROR: {e.code}")
            self.send_response(e.code)
            self.send_cors()
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(data)
        except Exception as e:
            self.send_json(500, {"error": str(e)})


if __name__ == "__main__":
    # Install websocket-client if needed
    try:
        import websocket
    except ImportError:
        import subprocess, sys
        subprocess.check_call([sys.executable, "-m", "pip", "install",
                               "websocket-client", "--break-system-packages", "-q"])
        import websocket

    mode = "DEMO (Paper Trading — no real money)" if USE_DEMO else "LIVE (Real Money ⚠)"
    print("=" * 58)
    print(f"  AlgoRhythm — Bybit Proxy")
    print(f"  Mode: {mode}")
    print(f"  URL:  {BASE_URL}")
    print(f"  Running on http://localhost:{PORT}")
    print(f"  Handles: Bybit Spot + Futures + WebSocket")
    if USE_DEMO:
        print(f"  ✅ DEMO MODE — safe to test, no real funds at risk")
        print(f"  To switch to live: set USE_DEMO = False at top of file")
    else:
        print(f"  ⚠  WARNING: REAL MONEY — trades cost real funds")
        print(f"  Start with small sizes to test.")
    print(f"  Keep this window open while trading.")
    print(f"  Press Ctrl+C to stop.")
    print("=" * 58)

    # Sync clock with Bybit server — fixes Windows clock drift (error 10002)
    print("\n  Syncing clock with Bybit server...")
    sync_bybit_time()
    print()

    # Re-sync every 10 minutes to handle ongoing drift
    def _periodic_time_sync():
        while True:
            time.sleep(600)
            sync_bybit_time()
    threading.Thread(target=_periodic_time_sync, daemon=True).start()



# ══════════════════════════════════════════════════════════════════════
# AUTONOMOUS TRADING ENGINE — runs on Render 24/7
# Scans Bybit every 5 minutes, places trades without any browser open
# Credentials stored via /set-credentials endpoint from the browser
# ══════════════════════════════════════════════════════════════════════

import math

# Stored credentials from browser login
_auto_creds = {
    'api_key': os.environ.get('BYBIT_API_KEY', ''),
    'api_secret': os.environ.get('BYBIT_API_SECRET', ''),
    'trading_mode': os.environ.get('TRADING_MODE', 'linear'),
    'trade_size': float(os.environ.get('TRADE_SIZE', '100')),
    'tp_pct': float(os.environ.get('TP_PCT', '15')),
    'sl_pct': float(os.environ.get('SL_PCT', '3')),
    'auto_enabled': os.environ.get('AUTO_TRADING', 'false').lower() == 'true',
    'min_confidence': float(os.environ.get('MIN_CONFIDENCE', '60')),
    'demo': os.environ.get('DEMO_MODE', 'true').lower() == 'true',
}
_open_positions = {}  # symbol -> position data
_last_regime = 'UNKNOWN'

# Trade management state — tracks peak profit for trailing
_trade_state = {}
# {symbol: {
#   'entry': float,       entry price
#   'side': str,          'Buy' or 'Sell'
#   'size': float,        trade size in USD
#   'peak_pnl': float,    highest P&L seen
#   'peak_price': float,  best price seen
#   'be_done': bool,      breakeven already set
# }}

def _bybit_request(method, path, params=None, body=None, api_key=None, api_secret=None):
    """Make authenticated Bybit API request from server side"""
    key = api_key or _auto_creds['api_key']
    secret = api_secret or _auto_creds['api_secret']
    if not key or not secret:
        return None
    try:
        ts = str(int(time.time() * 1000) + time_offset_ms)
        recv_window = '5000'
        if method == 'GET':
            query = urllib.parse.urlencode(params or {})
            sign_str = ts + key + recv_window + query
        else:
            sign_str = ts + key + recv_window + json.dumps(body or {})
        sig = hmac.new(secret.encode(), sign_str.encode(), hashlib.sha256).hexdigest()
        headers = {
            'X-BAPI-API-KEY': key,
            'X-BAPI-TIMESTAMP': ts,
            'X-BAPI-SIGN': sig,
            'X-BAPI-RECV-WINDOW': recv_window,
            'Content-Type': 'application/json',
        }
        url = _base() + path
        if method == 'GET' and params:
            url += '?' + urllib.parse.urlencode(params)
        data = json.dumps(body).encode() if body else None
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f'  [AutoTrader] API error {path}: {e}')
        return None

def _get_klines(symbol, interval, limit=100, category=None):
    """Fetch candles from Bybit"""
    try:
        cat = category or (_auto_creds['trading_mode'] if symbol.endswith('USDT') else 'spot')
        url = f'{BASE_URL}/v5/market/kline?symbol={symbol}&interval={interval}&limit={limit}&category={cat}'
        req = urllib.request.Request(url, headers={'User-Agent':'AlgoRhythm/1.0'})
        with urllib.request.urlopen(req, timeout=10) as r:
            d = json.loads(r.read())
        raw = d.get('result',{}).get('list',[])
        if not raw: return None
        candles = []
        for c in reversed(raw):
            candles.append({
                'open':float(c[1]),'high':float(c[2]),'low':float(c[3]),
                'close':float(c[4]),'volume':float(c[5])
            })
        return candles
    except Exception as e:
        return None

def _calc_adx(candles, period=14):
    if len(candles) < period*2+1: return None
    tr,pdm,mdm = [],[],[]
    for i in range(1,len(candles)):
        c,p = candles[i],candles[i-1]
        tr.append(max(c['high']-c['low'],abs(c['high']-p['close']),abs(c['low']-p['close'])))
        up,dn = c['high']-p['high'], p['low']-c['low']
        pdm.append(up if up>dn and up>0 else 0)
        mdm.append(dn if dn>up and dn>0 else 0)
    def wilder(arr,p):
        s=sum(arr[:p]); out=[s]
        for i in range(p,len(arr)): s=s-s/p+arr[i]; out.append(s)
        return out
    trs,ps,ms = wilder(tr,period),wilder(pdm,period),wilder(mdm,period)
    dx=[100*abs(100*ps[i]/trs[i]-100*ms[i]/trs[i])/((100*ps[i]/trs[i]+100*ms[i]/trs[i]) or 1) if trs[i] else 0 for i in range(len(trs))]
    if len(dx)<period: return None
    return sum(dx[-period:])/period

def _calc_ma(prices, period):
    if len(prices)<period: return None
    return sum(prices[-period:])/period

def _calc_rsi(prices, period=14):
    if len(prices)<period+1: return None
    gains,losses=0,0
    for i in range(len(prices)-period,len(prices)):
        d=prices[i]-prices[i-1]
        if d>0: gains+=d
        else: losses-=d
    ag,al=gains/period,losses/period
    if al==0: return 100
    return 100-100/(1+ag/al)

def _check_regime():
    global _last_regime
    try:
        c4h = _get_klines('BTCUSDT','240',100)
        if not c4h or len(c4h)<50: return _last_regime
        closes=[c['close'] for c in c4h]
        adx=_calc_adx(c4h,14)
        ma50=_calc_ma(closes,50)
        ma50p=_calc_ma(closes[:-5],50)
        slope_up = ma50 > ma50p if ma50 and ma50p else False
        cur=closes[-1]
        if not adx or adx<18:
            regime='RANGING'
        elif adx<22:
            regime='TRENDING_UP' if (slope_up and cur>ma50) else ('TRENDING_DOWN' if (not slope_up and cur<ma50) else 'RANGING')
        else:
            regime='TRENDING_UP' if (slope_up and cur>ma50) else ('TRENDING_DOWN' if (not slope_up and cur<ma50) else 'RANGING')
        if regime != _last_regime:
            print(f'  [AutoTrader] 🌡️ Regime changed: {_last_regime} → {regime} | ADX={adx:.1f}')
        _last_regime = regime
        return regime
    except Exception as e:
        return _last_regime


# ═══════════════════════════════════════════════════════════════
# FULL ANALYSIS ENGINE — exact translation of browser bot
# ═══════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════
# SERVER ANALYSIS ENGINE — exact Python translation of browser bot
# getSignal() produces identical results to the browser's analysis
# ═══════════════════════════════════════════════════════════════════
import math

# ── MATH HELPERS ──────────────────────────────────────────────────
def _sma(arr, n):
    if len(arr) < n: return None
    return sum(arr[-n:]) / n

def _ema(closes, period):
    if len(closes) < period: return None
    k = 2 / (period + 1)
    e = sum(closes[:period]) / period
    for v in closes[period:]:
        e = v * k + e * (1 - k)
    return e

def _wma(closes, period):
    if len(closes) < period: return None
    s = closes[-period:]
    total = sum(v * (i+1) for i,v in enumerate(s))
    w = sum(i+1 for i in range(period))
    return total / w

def _rsi(closes, period=14):
    if len(closes) < period+1: return 50
    gains = losses = 0
    for i in range(len(closes)-period, len(closes)):
        d = closes[i] - closes[i-1]
        if d > 0: gains += d
        else: losses -= d
    ag, al = gains/period, losses/period
    return 100 if al == 0 else 100 - 100/(1+ag/al)

def _adx(candles, period=14):
    if len(candles) < period*2: return None
    tr, pdm, mdm = [], [], []
    for i in range(1, len(candles)):
        c, p = candles[i], candles[i-1]
        tr.append(max(c['h']-c['l'], abs(c['h']-p['c']), abs(c['l']-p['c'])))
        up = c['h'] - p['h']; dn = p['l'] - c['l']
        pdm.append(up if up > dn and up > 0 else 0)
        mdm.append(dn if dn > up and dn > 0 else 0)
    def wilder(arr, p):
        s = sum(arr[:p]); out = [s]
        for v in arr[p:]: s = s - s/p + v; out.append(s)
        return out
    trs, ps, ms = wilder(tr,period), wilder(pdm,period), wilder(mdm,period)
    dx = []
    for i in range(len(trs)):
        pi = (ps[i]/trs[i]*100) if trs[i] else 0
        mi = (ms[i]/trs[i]*100) if trs[i] else 0
        dx.append(abs(pi-mi)/((pi+mi) or 1)*100)
    return wilder(dx, period)[-1] if len(dx) >= period else None

def _macd(closes):
    if len(closes) < 26: return None
    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26)
    if ema12 is None or ema26 is None: return None
    line = ema12 - ema26
    return {'line': line, 'bullish': line > 0}

def _obv(candles, lookback=20):
    if len(candles) < 2: return 0
    obv = 0
    for i in range(1, len(candles)):
        if candles[i]['c'] > candles[i-1]['c']:   obv += candles[i].get('v',0)
        elif candles[i]['c'] < candles[i-1]['c']:  obv -= candles[i].get('v',0)
    return obv

def _lin_reg_slope(closes, period):
    if len(closes) < period: return 0
    s = closes[-period:]; n = period
    sX = sY = sXY = sX2 = 0
    for i, y in enumerate(s):
        x = i+1; sX+=x; sY+=y; sXY+=x*y; sX2+=x*x
    d = n*sX2 - sX*sX
    return 0 if d == 0 else (n*sXY - sX*sY)/d

def _pearson(closes, period):
    if len(closes) < period: return 0
    s = closes[-period:]; n = period
    sX=sY=sXY=sX2=sY2 = 0
    for i, y in enumerate(s):
        x=i+1; sX+=x; sY+=y; sXY+=x*y; sX2+=x*x; sY2+=y*y
    num = n*sXY - sX*sY
    den = math.sqrt((n*sX2-sX*sX)*(n*sY2-sY*sY))
    return 0 if den == 0 else num/den

def _support_resistance(candles):
    if not candles or len(candles) < 10:
        return {'support':[], 'resistance':[]}
    n = len(candles)
    support, resistance = [], []
    for i in range(2, n-2):
        lo = candles[i]['l']
        if lo < candles[i-1]['l'] and lo < candles[i-2]['l'] and lo < candles[i+1]['l'] and lo < candles[i+2]['l']:
            support.append(lo)
        hi = candles[i]['h']
        if hi > candles[i-1]['h'] and hi > candles[i-2]['h'] and hi > candles[i+1]['h'] and hi > candles[i+2]['h']:
            resistance.append(hi)
    return {'support': sorted(set(round(s,8) for s in support)),
            'resistance': sorted(set(round(r,8) for r in resistance))}

def _detect_trend(candles):
    if not candles or len(candles) < 10: return 'UNKNOWN'
    closes = [c['c'] for c in candles]
    ma20 = _sma(closes, 20)
    ma50 = _sma(closes, min(50, len(closes)))
    cur  = closes[-1]
    if not ma20: return 'UNKNOWN'
    slope = _lin_reg_slope(closes, min(20, len(closes)))
    if slope > 0 and cur > ma20 and (ma50 is None or ma20 > ma50): return 'UPTREND'
    if slope < 0 and cur < ma20 and (ma50 is None or ma20 < ma50): return 'DOWNTREND'
    return 'SIDEWAYS'

def _detect_all_patterns(candles):
    NONE = {'name':'None','direction':0,'strength':1}
    if not candles or len(candles) < 5: return NONE
    n = len(candles)
    c1,c2,c3,c4 = candles[n-1],candles[n-2],candles[n-3],candles[n-4] if n>=4 else candles[0]

    def body(c): return abs(c['c']-c['o'])
    def rng(c):  return max(c['h']-c['l'], 0.000001)
    def hiW(c):  return c['h'] - max(c['o'],c['c'])
    def loW(c):  return min(c['o'],c['c']) - c['l']
    def bull(c): return c['c'] > c['o']
    def bear(c): return c['c'] < c['o']
    def bRat(c): return body(c)/rng(c)

    WR=2.0; BMIN=0.60; DOJI=0.10

    # Bullish
    if loW(c1)>=body(c1)*WR and hiW(c1)<body(c1)*0.5 and bRat(c1)<0.4 and not bull(c2):
        return {'name':'Hammer','direction':1,'strength':2}
    if hiW(c1)>=body(c1)*WR and loW(c1)<body(c1)*0.5 and bRat(c1)<0.4 and bear(c2):
        return {'name':'Inverted Hammer','direction':1,'strength':1}
    if bull(c1) and bear(c2) and c1['o']<c2['c'] and c1['c']>c2['o'] and body(c1)>body(c2):
        return {'name':'Bullish Engulfing','direction':1,'strength':3}
    if bull(c1) and bear(c2) and c1['o']<c2['l'] and c1['c']>c2['o']+body(c2)*0.5 and c1['c']<c2['o']:
        return {'name':'Piercing Line','direction':1,'strength':2}
    if bear(c3) and body(c2)<body(c3)*0.5 and bull(c1) and c1['c']>c3['o']+body(c3)*0.5:
        return {'name':'Morning Star','direction':1,'strength':3}
    if bull(c1) and bull(c2) and bull(c3) and c1['c']>c2['c'] and c2['c']>c3['c'] and bRat(c1)>=BMIN and bRat(c2)>=BMIN:
        return {'name':'Three White Soldiers','direction':1,'strength':3}
    if n>=4 and bull(c1) and bear(c3) and c1['c']>c4['c'] and c1['o']<c3['c'] and c2['h']<c4['c']:
        return {'name':'Rising Three Methods','direction':1,'strength':2}
    # Bearish
    if loW(c1)>=body(c1)*WR and hiW(c1)<body(c1)*0.5 and bRat(c1)<0.4 and bull(c2):
        return {'name':'Hanging Man','direction':-1,'strength':2}
    if hiW(c1)>=body(c1)*WR and loW(c1)<body(c1)*0.5 and bRat(c1)<0.4 and bull(c2):
        return {'name':'Shooting Star','direction':-1,'strength':2}
    if bear(c1) and bull(c2) and c1['o']>c2['c'] and c1['c']<c2['o'] and body(c1)>body(c2):
        return {'name':'Bearish Engulfing','direction':-1,'strength':3}
    if bear(c1) and bull(c2) and c1['o']>c2['h'] and c1['c']<c2['o']+body(c2)*0.5 and c1['c']>c2['o']:
        return {'name':'Dark Cloud Cover','direction':-1,'strength':2}
    if bull(c3) and body(c2)<body(c3)*0.5 and bear(c1) and c1['c']<c3['o']+body(c3)*0.5:
        return {'name':'Evening Star','direction':-1,'strength':3}
    if bear(c1) and bear(c2) and bear(c3) and c1['c']<c2['c'] and c2['c']<c3['c'] and bRat(c1)>=BMIN and bRat(c2)>=BMIN:
        return {'name':'Three Black Crows','direction':-1,'strength':3}
    if n>=4 and bear(c1) and bull(c3) and c1['c']<c4['c'] and c1['o']>c3['c'] and c2['l']>c4['c']:
        return {'name':'Falling Three Methods','direction':-1,'strength':2}
    # Neutral
    if bRat(c1)<=DOJI and loW(c1)>=rng(c1)*0.6 and hiW(c1)<rng(c1)*0.1:
        return {'name':'Dragonfly Doji','direction':1,'strength':2}
    if bRat(c1)<=DOJI and hiW(c1)>=rng(c1)*0.6 and loW(c1)<rng(c1)*0.1:
        return {'name':'Gravestone Doji','direction':-1,'strength':2}
    if bRat(c1)<=DOJI:
        return {'name':'Doji','direction':0,'strength':1}
    if bRat(c1)<0.3 and loW(c1)>body(c1)*0.5 and hiW(c1)>body(c1)*0.5:
        return {'name':'Spinning Top','direction':0,'strength':1}
    return NONE


def server_get_signal(symbol, c30m, c4h, c5m=None, c1h=None):
    """
    Exact Python translation of browser's getSignal() function.
    Inputs: candle dicts with keys o,h,l,c,v
    Returns: dict with signal, confidence, direction, reason, patternName
    """
    HOLD = lambda r: {'signal':'HOLD','confidence':50,'direction':0,'reason':r,'patternName':'None'}

    candles = c30m
    if not candles or len(candles) < 30:
        return HOLD('Insufficient 30m candle history')

    closes  = [c['c'] for c in candles]
    volumes = [c.get('v',0) for c in candles]
    n       = len(closes)
    cur     = closes[-1]
    reasons = []
    score   = 0

    # ── LAYER 1: HARD GATES ───────────────────────────────────────
    # Gate A: Breakout
    slice22 = candles[-22:-2]
    range_high = max(c['h'] for c in slice22)
    range_low  = min(c['l'] for c in slice22)
    up_break   = closes[-1] > range_high
    dn_break   = closes[-1] < range_low
    if not up_break and not dn_break:
        return HOLD('Price inside 20-bar range — no breakout detected')
    direction = 1 if up_break else -1

    # Gate B: S&R proximity
    sr = _support_resistance(c1h if c1h and len(c1h)>=10 else candles)
    near_sr = (sr['support'][-1] if sr['support'] else None) if direction==1 \
              else (sr['resistance'][0] if sr['resistance'] else None)
    if not near_sr:
        return HOLD(f'No {"support" if direction==1 else "resistance"} level near price')
    sr_dist = abs(cur - near_sr) / cur * 100
    if sr_dist > 8:
        return HOLD(f'Price {sr_dist:.1f}% from nearest S&R (need <8%)')
    score += 15
    reasons.append(f'S&R Gate (+15): Level {near_sr:.6f} is {sr_dist:.1f}% away')

    # Gate C: ADX >= 22
    adx = _adx(candles, 14)
    if not adx or adx < 22:
        return HOLD(f'ADX {adx:.1f if adx else "N/A"} below 22 — market ranging')

    # Gate D: MA20 alignment
    ma20 = _sma(closes, 20)
    ma_ok = (cur > ma20) if direction==1 else (cur < ma20)
    if not ma_ok:
        return HOLD(f'{"Bullish" if direction==1 else "Bearish"} breakout but price {"below" if direction==1 else "above"} MA20')

    # Gate E: H4 alignment
    if c4h and len(c4h) >= 20:
        cl4   = [c['c'] for c in c4h]
        h4ma  = _sma(cl4, 50)
        h4last= cl4[-1]
        if h4ma:
            if direction==1 and h4last < h4ma:
                return HOLD('30m bullish breakout but 4H trend conflicts')
            if direction==-1 and h4last > h4ma:
                return HOLD('30m bearish breakout but 4H trend conflicts')

    score += 12
    reasons.append(f'Trend Gate (+12): ADX {adx:.1f} | Price {"above" if direction==1 else "below"} MA20 | H4 aligned')
    score += 25
    reasons.append(f'Breakout (+25): Closed {"ABOVE" if up_break else "BELOW"} 20-bar range')

    # Multi-TF bias
    trends = []
    for tf_candles in [c5m, c30m, c1h, c4h]:
        if tf_candles and len(tf_candles) >= 10:
            trends.append(_detect_trend(tf_candles))
    up_count   = trends.count('UPTREND')
    down_count = trends.count('DOWNTREND')
    ts = 'STRONG_UP' if up_count>=3 else 'WEAK_UP' if up_count==2 else \
         'STRONG_DOWN' if down_count>=3 else 'WEAK_DOWN' if down_count==2 else 'MIXED'

    bias_up   = ts in ('STRONG_UP','WEAK_UP')
    bias_down = ts in ('STRONG_DOWN','WEAK_DOWN')
    if direction==1 and bias_down:
        return HOLD(f'Multi-TF bias is BEARISH ({ts}) — blocks bullish entry')
    if direction==-1 and bias_up:
        return HOLD(f'Multi-TF bias is BULLISH ({ts}) — blocks bearish entry')
    if (direction==1 and bias_up) or (direction==-1 and bias_down):
        score += 12
        reasons.append(f'Multi-TF Bias (+12): {ts} confirms direction')

    # ── LAYER 2: TECHNICAL SCORING ────────────────────────────────
    # MAs (5 votes × 3pts)
    sma20p = _sma(closes[:-1], 20)
    sma50  = _sma(closes, 50)
    ema8   = _ema(closes, 8)
    ema21  = _ema(closes, 21)
    wma21  = _wma(closes, 21)
    ma_v   = 0
    if direction==1:
        if ma20 and cur > ma20:  ma_v+=1
        if ma20 and sma50 and ma20>sma50: ma_v+=1
        if ema8 and ema21 and ema8>ema21: ma_v+=1
        if wma21 and cur>wma21: ma_v+=1
        if ma20 and sma20p and ma20>sma20p: ma_v+=1
    else:
        if ma20 and cur<ma20:   ma_v+=1
        if ma20 and sma50 and ma20<sma50: ma_v+=1
        if ema8 and ema21 and ema8<ema21: ma_v+=1
        if wma21 and cur<wma21: ma_v+=1
        if ma20 and sma20p and ma20<sma20p: ma_v+=1
    score += ma_v * 3
    reasons.append(f'MA Score (+{ma_v*3}): {ma_v}/5 MAs aligned')

    # Linear Regression slope
    slope     = _lin_reg_slope(closes, 20)
    slope_up  = slope > 0
    slope_pct = abs(slope) / (cur or 1) * 100
    if (direction==1 and slope_up) or (direction==-1 and not slope_up):
        pts = 15 if slope_pct>0.1 else 10 if slope_pct>0.03 else 5
        score += pts
        reasons.append(f'LinReg (+{pts}): Slope {"UP" if slope_up else "DOWN"} {slope_pct:.3f}%/bar')
    else:
        score -= 8
        reasons.append('LinReg (-8): Slope opposes direction')

    # Pearson Correlation
    corr = _pearson(closes, 20)
    ca   = abs(corr)
    corr_dir = 1 if corr > 0 else -1
    if corr_dir == direction:
        pts = 12 if ca>=0.8 else 8 if ca>=0.6 else 4 if ca>=0.4 else 0
        score += pts
        if pts: reasons.append(f'Correlation (+{pts}): r={corr:.2f}')
    elif ca < 0.3:
        score -= 8
        reasons.append(f'Correlation (-8): r={corr:.2f} choppy')
    else:
        score -= 5

    # RSI
    rsi_val = _rsi(closes, 14)
    if direction==1 and rsi_val < 70:
        p = 15 if rsi_val<60 else 8
        score += p
        reasons.append(f'RSI (+{p}): {rsi_val:.0f} room to run')
    elif direction==-1 and rsi_val > 30:
        p = 15 if rsi_val>40 else 8
        score += p
        reasons.append(f'RSI (+{p}): {rsi_val:.0f} room to fall')
    else:
        score -= 15
        reasons.append(f'RSI (-15): {rsi_val:.0f} overbought/oversold vs direction')

    # ADX strength bonus
    ap = 8 if adx>=30 else 4 if adx>=22 else 0
    if ap:
        score += ap
        reasons.append(f'ADX (+{ap}): {adx:.0f} strength')

    # MACD
    macd_d = _macd(closes)
    if macd_d:
        if (direction==1 and macd_d['bullish']) or (direction==-1 and not macd_d['bullish']):
            score += 10; reasons.append('MACD (+10): Momentum confirmed')
        else:
            score -= 12; reasons.append('MACD (-12): Momentum opposes')

    # OBV
    obv_val = _obv(candles, 20)
    if (direction==1 and obv_val>0) or (direction==-1 and obv_val<0):
        score += 8; reasons.append('OBV (+8): Volume pressure confirms')
    elif obv_val != 0:
        score -= 8; reasons.append('OBV (-8): Volume pressure opposes')

    # EMA cross
    if ema8 and ema21:
        if (direction==1 and ema8>ema21) or (direction==-1 and ema8<ema21): score += 6
        else: score -= 6

    # S&R support/block
    sr_help  = (sr['support'][-1]   if sr['support']   else None) if direction==1 else (sr['resistance'][0] if sr['resistance'] else None)
    sr_block = (sr['resistance'][0] if sr['resistance'] else None) if direction==1 else (sr['support'][-1]   if sr['support']   else None)
    if sr_help:
        d = abs(cur-sr_help)/cur*100
        if d<=3: score+=10; reasons.append(f'S&R Support (+10): {sr_help:.6f} {d:.1f}% away')
    if sr_block:
        d = abs(cur-sr_block)/cur*100
        if d<=6: score-=15; reasons.append(f'S&R Block (-15): {sr_block:.6f} only {d:.1f}% ahead')

    # Volume spike
    cv = volumes[-1]; av = sum(volumes[-21:-1])/20 if len(volumes)>=21 else 0
    if av > 0 and cv/av >= 1.5:
        score += 8; reasons.append(f'Volume (+8): {cv/av:.1f}× average')

    score = max(0, min(100, score))

    # ── LAYER 3: CANDLESTICK PATTERNS ─────────────────────────────
    pat = _detect_all_patterns(candles)
    if pat['name'] != 'None':
        if pat['direction'] != 0:
            if pat['direction'] == direction:
                b = round(20 * pat['strength'] / 2)
                score += b
                reasons.append(f'Pattern (+{b}): {pat["name"]} CONFIRMS {"BUY" if direction==1 else "SELL"}')
            else:
                p = round(10 * pat['strength'] / 2)
                score -= p
                reasons.append(f'Pattern (-{p}): {pat["name"]} opposes direction')
        else:
            score -= 8
            reasons.append(f'Pattern (-8): {pat["name"]} — indecision')

    score = max(0, min(100, score))

    # ── CONFLICT VETO ──────────────────────────────────────────────
    conflicts = 0
    if macd_d and ((1 if macd_d['bullish'] else -1) != direction): conflicts+=1
    if obv_val != 0 and ((1 if obv_val>0 else -1) != direction): conflicts+=1
    if ema8 and ema21 and ((1 if ema8>ema21 else -1) != direction): conflicts+=1
    if bias_up   and direction==-1: conflicts+=1
    if bias_down and direction==1:  conflicts+=1
    if conflicts >= 3:
        return HOLD(f'{conflicts} opposing signals — conflict veto')
    if conflicts == 2:
        score = round(score * 0.8)
        reasons.append(f'Conflict penalty: {conflicts} opposing signals — score reduced')

    score = max(0, min(100, score))

    final_signal = ('BUY' if direction==1 else 'SELL') if score >= 60 else 'HOLD'
    top_reasons  = ' | '.join(reasons[:3])

    return {
        'signal':      final_signal,
        'confidence':  score,
        'direction':   direction,
        'reason':      f'Score {score}/100 | {pat["name"]+" | " if pat["name"]!="None" else ""}{top_reasons}',
        'patternName': pat['name'],
        'reasons':     reasons,
        'adx':         adx,
        'rsi':         round(rsi_val, 1),
    }


def _scan_symbol(symbol, category=None):
    """
    Uses the SAME analysis engine as the browser bot (getSignal equivalent).
    Returns signal dict with confidence, direction, reason etc.
    """
    # Skip stablecoins
    STABLECOINS = {
        'USDCUSDT','USDEUSDT','USD1USDT','USDTUSDC','BUSD','TUSD','DAI',
        'USDC','USDT','USDE','USD1','USDTEUR','USDTBRL','USDCEUR',
    }
    if symbol in STABLECOINS: return None
    if symbol.startswith('USDC') or symbol.startswith('USDE') or symbol.startswith('USD1'):
        return None

    try:
        cat = category or (_auto_creds['trading_mode'] if symbol.endswith('USDT') else 'spot')

        # Fetch same timeframes as browser: 30m, 4h, 1h (5m optional)
        c30m = _get_klines(symbol, '30',  200, category=cat)
        c4h  = _get_klines(symbol, '240', 100, category=cat)
        c1h  = _get_klines(symbol, '60',  100, category=cat)

        if not c30m or len(c30m) < 30:
            return None

        # Convert to format expected by analysis engine {o,h,l,c,v}
        def to_ohlcv(raw):
            if not raw: return None
            return [{'o':c['open'],'h':c['high'],'l':c['low'],'c':c['close'],'v':c['volume']} for c in raw]

        result = server_get_signal(
            symbol,
            to_ohlcv(c30m),
            to_ohlcv(c4h),
            c5m=None,
            c1h=to_ohlcv(c1h),
        )

        if not result or result['signal'] == 'HOLD':
            return None

        return {
            'symbol':      symbol,
            'direction':   result['direction'],
            'score':       result['confidence'],
            'signal':      result['signal'],
            'adx':         result.get('adx', 0),
            'rsi':         result.get('rsi', 50),
            'pattern':     result.get('patternName','None'),
            'reason':      result.get('reason',''),
            'price':       c30m[-1]['close'],
            'category':    cat,
        }

    except Exception as e:
        print(f'  [Scan] Error {symbol}: {e}')
        return None


def _place_order(symbol, side, size, price, category=None):
    """Place a market order on Bybit"""
    try:
        mode = category or (_auto_creds['trading_mode'] if symbol.endswith('USDT') else 'spot')
        # Get instrument info for qty step
        info_r=_bybit_request('GET','/v5/market/instruments-info',{'category':mode,'symbol':symbol})
        qty_step=0.001
        min_notional=1
        if info_r and info_r.get('retCode')==0:
            lst=info_r.get('result',{}).get('list',[])
            if lst:
                lot=lst[0].get('lotSizeFilter',{})
                qty_step=float(lot.get('qtyStep',0.001))
                min_notional=float(lot.get('minOrderAmt',1))
        raw_qty=size/price
        qty=math.floor(raw_qty/qty_step)*qty_step
        step_decs=len(str(qty_step).rstrip('0').split('.')[-1]) if '.' in str(qty_step) else 0
        qty=round(qty,step_decs)
        if qty*price<max(min_notional,0.5):
            print(f'  [AutoTrader] {symbol}: order too small ({qty*price:.2f} < {min_notional})')
            return False
        body={'category':mode,'symbol':symbol,'side':side,'orderType':'Market','qty':str(qty)}
        if mode=='linear':
            body['leverage']='1'
            # SAFETY NET: attach an exchange-native stop loss so Bybit itself
            # protects the position even if this server sleeps or crashes.
            # The management loop still trails it tighter while running.
            try:
                sl_pct = float(_auto_creds.get('sl_pct', 3) or 3)
                hard_sl_pct = max(sl_pct, 2.0)  # never tighter than 2%
                if side == 'Buy':
                    sl_price = price * (1 - hard_sl_pct / 100.0)
                else:
                    sl_price = price * (1 + hard_sl_pct / 100.0)
                body['stopLoss'] = f'{sl_price:.8f}'.rstrip('0').rstrip('.')
                body['slTriggerBy'] = 'MarkPrice'
            except Exception as _sl_e:
                print(f'  [AutoTrader] {symbol}: could not attach safety SL: {_sl_e}')
        r=_bybit_request('POST','/v5/order/create',body=body)
        if r and r.get('retCode')==0:
            actual_notional = qty * price
            print(f'  [AutoTrader] ✅ {side} {symbol}: {qty} units @ ~${price:.4f} | Notional: ${actual_notional:.2f}')
            # Record in trade state for management
            _trade_state[symbol] = {
                'entry':      price,
                'side':       side,
                'size':       qty,
                'peak_pnl':   0.0,
                'peak_price': price,
                'be_done':    False,
            }
            return True
        else:
            print(f'  [AutoTrader] ❌ Order failed {symbol}: {r}')
            return False
    except Exception as e:
        print(f'  [AutoTrader] Order error {symbol}: {e}')
        return False


def _set_sl(symbol, new_sl, category='linear'):
    """Set stop loss on an open position via Bybit API"""
    try:
        r = _bybit_request('POST', '/v5/position/trading-stop', body={
            'category': category,
            'symbol':   symbol,
            'stopLoss': str(round(new_sl, 8)),
            'slTriggerBy': 'MarkPrice',
            'tpslMode': 'Full',
            'positionIdx': 0,
        })
        if r and r.get('retCode') == 0:
            return True
        else:
            print(f'  [TradeMgmt] SL set failed {symbol}: {r}')
            return False
    except Exception as e:
        print(f'  [TradeMgmt] SL error {symbol}: {e}')
        return False

def _close_position_market(symbol, side, qty, category='linear'):
    """Close position at market price"""
    try:
        close_side = 'Sell' if side == 'Buy' else 'Buy'
        r = _bybit_request('POST', '/v5/order/create', body={
            'category':    category,
            'symbol':      symbol,
            'side':        close_side,
            'orderType':   'Market',
            'qty':         str(qty),
            'reduceOnly':  True,
            'timeInForce': 'IOC',
        })
        if r and r.get('retCode') == 0:
            print(f'  [TradeMgmt] ✅ Closed {symbol} at market')
            return True
        else:
            print(f'  [TradeMgmt] Close failed {symbol}: {r}')
            return False
    except Exception as e:
        print(f'  [TradeMgmt] Close error {symbol}: {e}')
        return False

def _manage_open_positions():
    """
    Trade management — runs every 60 seconds
    For each open position:
      1. Breakeven: move SL to entry after +1% profit
      2. Trailing SL: trail SL at 1.5x ATR behind price
      3. Trailing TP: close if profit drops $1 or 2% from peak
    """
    global _trade_state

    try:
        positions = _get_positions()
    except Exception as e:
        print(f'  [TradeMgmt] Could not fetch positions: {e}')
        return

    if not positions:
        return

    for symbol, pos in positions.items():
        try:
            side      = pos.get('side', 'Buy')
            entry     = float(pos.get('avgPrice', 0) or pos.get('entryPrice', 0))
            size      = float(pos.get('size', 0))
            cur_sl    = float(pos.get('stopLoss', 0) or 0)
            unreal_pnl= float(pos.get('unrealisedPnl', 0))
            mark_price= float(pos.get('markPrice', 0) or entry)
            category  = _auto_creds.get('trading_mode', 'linear')

            if entry <= 0 or size <= 0:
                continue

            # Init trade state for this symbol
            if symbol not in _trade_state:
                _trade_state[symbol] = {
                    'entry':       entry,
                    'side':        side,
                    'size':        size,
                    'peak_pnl':    unreal_pnl,
                    'peak_price':  mark_price,
                    'be_done':     False,
                }

            state = _trade_state[symbol]

            # Update peak values
            if unreal_pnl > state['peak_pnl']:
                state['peak_pnl']   = unreal_pnl
                state['peak_price'] = mark_price

            is_buy = side == 'Buy'

            # ── 0. SAFETY NET: ensure an exchange-native SL exists ──
            # Positions opened before this update (or whose SL was cleared)
            # get a hard stop registered on Bybit itself, so they stay
            # protected even if this server sleeps, restarts, or crashes.
            if cur_sl == 0:
                try:
                    sl_pct   = max(float(_auto_creds.get('sl_pct', 3) or 3), 2.0)
                    sl_price = entry * (1 - sl_pct / 100.0) if is_buy else entry * (1 + sl_pct / 100.0)
                    r = _bybit_request('POST', '/v5/position/trading-stop', body={
                        'category': category,
                        'symbol':   symbol,
                        'stopLoss': str(round(sl_price, 8)),
                        'slTriggerBy': 'MarkPrice',
                        'tpslMode': 'Full',
                        'positionIdx': 0,
                    })
                    if r and r.get('retCode') == 0:
                        print(f'  [TradeMgmt] 🛡 Safety SL backfilled for {symbol} @ {sl_price:.6f}')
                        cur_sl = sl_price
                    else:
                        print(f'  [TradeMgmt] Safety SL rejected for {symbol}: {r}')
                except Exception as e:
                    print(f'  [TradeMgmt] Safety SL error {symbol}: {e}')

            # ── 1. BREAKEVEN ─────────────────────────────────
            # Move SL to entry when price moves +1% in our favour
            if not state['be_done']:
                pnl_pct = (mark_price - entry) / entry * 100 if is_buy                           else (entry - mark_price) / entry * 100
                if pnl_pct >= 1.0:
                    be_price = entry
                    # Only move SL if it's currently worse than entry
                    if is_buy and (cur_sl < be_price or cur_sl == 0):
                        if _set_sl(symbol, be_price, category):
                            state['be_done'] = True
                            print(f'  [TradeMgmt] 🔒 BREAKEVEN {symbol} SL moved to entry ${be_price:.6f} (was ${cur_sl:.6f})')
                    elif not is_buy and (cur_sl > be_price or cur_sl == 0):
                        if _set_sl(symbol, be_price, category):
                            state['be_done'] = True
                            print(f'  [TradeMgmt] 🔒 BREAKEVEN {symbol} SL moved to entry ${be_price:.6f} (was ${cur_sl:.6f})')

            # ── 2. TRAILING SL ───────────────────────────────
            # Trail SL 1.5% behind current price as it moves in profit
            # Only trail if already in profit
            if unreal_pnl > 0:
                trail_distance = mark_price * 0.015  # 1.5% trail
                if is_buy:
                    new_trail_sl = mark_price - trail_distance
                    # Only move SL up, never down
                    if new_trail_sl > (cur_sl or 0):
                        if _set_sl(symbol, new_trail_sl, category):
                            print(f'  [TradeMgmt] 📈 TRAIL SL {symbol} → ${new_trail_sl:.6f} (price=${mark_price:.6f})')
                            cur_sl = new_trail_sl
                else:
                    new_trail_sl = mark_price + trail_distance
                    # Only move SL down, never up
                    if cur_sl == 0 or new_trail_sl < cur_sl:
                        if _set_sl(symbol, new_trail_sl, category):
                            print(f'  [TradeMgmt] 📉 TRAIL SL {symbol} → ${new_trail_sl:.6f} (price=${mark_price:.6f})')
                            cur_sl = new_trail_sl

            # ── 3. TRAILING TP ───────────────────────────────
            # Close position if profit drops $2 OR 2% from peak
            # Only applies after:
            #   a) peak profit has reached at least $2
            #   b) position has been tracked for at least 3 cycles (3 min)
            state['cycles'] = state.get('cycles', 0) + 1
            peak = state['peak_pnl']

            # Only activate trail TP after position is profitable AND mature
            if peak >= 2.0 and state['cycles'] >= 3:
                drop_dollar = peak - unreal_pnl
                drop_pct    = drop_dollar / peak * 100 if peak > 0 else 0

                # Trigger: profit dropped $2 from peak OR dropped 3% from peak
                trigger = drop_dollar >= 2.0 or drop_pct >= 3.0

                if trigger:
                    print(f'  [TradeMgmt] 🎯 TRAIL TP {symbol} | Peak=${peak:.2f} Now=${unreal_pnl:.2f} Drop=${drop_dollar:.2f} ({drop_pct:.1f}%) — closing')
                    if _close_position_market(symbol, side, str(size), category):
                        if symbol in _trade_state:
                            del _trade_state[symbol]
                        if symbol in _open_positions:
                            del _open_positions[symbol]

        except Exception as e:
            print(f'  [TradeMgmt] Error managing {symbol}: {e}')

def _trade_management_loop():
    """Separate thread — monitors positions every 60 seconds"""
    time.sleep(45)  # Stagger after main loop starts
    print('  [TradeMgmt] 🛡️ Trade management loop started (60s interval)')
    print('  [TradeMgmt] Features: Breakeven +1% | Trail SL 1.5% | Trail TP $1 or 2% from peak')
    while True:
        try:
            _req_ctx.demo = _auto_creds.get('demo')
            if _auto_creds.get('api_key') and _auto_creds.get('api_secret'):
                _manage_open_positions()
        except Exception as e:
            print(f'  [TradeMgmt] Loop error: {e}')
        time.sleep(60)  # Check every 60 seconds


# ═══════════════════════════════════════════════════════
# TELEGRAM SIGNAL ALERTS
# Set in Render env:  TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID
# Strong signals (>= MIN_CONFIDENCE) are pushed to the channel.
# Per-symbol cooldown prevents repeat spam every 5-min scan.
# ═══════════════════════════════════════════════════════
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '').strip()
TELEGRAM_CHAT_ID   = os.environ.get('TELEGRAM_CHAT_ID', '').strip()
TG_COOLDOWN_SECS   = int(os.environ.get('TELEGRAM_COOLDOWN_SECS', '3600'))  # 1h per symbol
_tg_last_alert = {}

# ── Alpaca news (served to the website; keys stay server-side) ────────
ALPACA_NEWS_KEY    = os.environ.get('ALPACA_NEWS_KEY',    'PKAJDQC5LZGYDHQ3S5IITANP5G')
ALPACA_NEWS_SECRET = os.environ.get('ALPACA_NEWS_SECRET', '6AMdh68737pBN3izqfxFMfNoWkSt4YYquvmvDH6dRxUf')
_news_cache = {'ts': 0, 'data': None}

def _fetch_alpaca_news(limit=10):
    """Fetch latest news from Alpaca, cached for 5 minutes."""
    now = time.time()
    if _news_cache['data'] is not None and now - _news_cache['ts'] < 300:
        return _news_cache['data']
    try:
        req = urllib.request.Request(
            f'https://data.alpaca.markets/v1beta1/news?limit={int(limit)}&sort=desc',
            headers={
                'APCA-API-KEY-ID': ALPACA_NEWS_KEY,
                'APCA-API-SECRET-KEY': ALPACA_NEWS_SECRET,
            })
        with urllib.request.urlopen(req, timeout=10, context=ssl_ctx) as r:
            raw = json.loads(r.read())
        items = [{
            'headline':   n.get('headline', ''),
            'source':     n.get('source', 'news'),
            'url':        n.get('url', ''),
            'created_at': n.get('created_at', ''),
        } for n in raw.get('news', [])]
        _news_cache['ts'] = now
        _news_cache['data'] = {'news': items}
        return _news_cache['data']
    except Exception as e:
        print(f'  [News] Alpaca fetch error: {e}')
        return _news_cache['data'] or {'news': [], 'error': str(e)}

# ── Stripe subscription verification ──────────────────────────────────
# Set in Render env: STRIPE_SECRET_KEY (an sk_live_... or restricted rk_ key)
# The website asks /check-subscription?email=... and this server queries
# Stripe directly. No database needed — Stripe is the source of truth.
STRIPE_SECRET_KEY = os.environ.get('STRIPE_SECRET_KEY', '').strip()
_stripe_cache = {}  # email -> (ts, result)

def _stripe_get(path, params=None):
    qs  = urllib.parse.urlencode(params or {})
    url = 'https://api.stripe.com' + path + ('?' + qs if qs else '')
    req = urllib.request.Request(url, headers={
        'Authorization': 'Bearer ' + STRIPE_SECRET_KEY})
    with urllib.request.urlopen(req, timeout=15, context=ssl_ctx) as r:
        return json.loads(r.read())

def _check_stripe_subscription(email):
    """Returns {active, plan, current_period_end} for the email, cached 60s."""
    email = (email or '').strip().lower()
    now = time.time()
    cached = _stripe_cache.get(email)
    if cached and now - cached[0] < 60:
        return cached[1]
    result = {'active': False}
    try:
        customers = _stripe_get('/v1/customers', {'email': email, 'limit': 5}).get('data', [])
        for cust in customers:
            subs = _stripe_get('/v1/subscriptions', {
                'customer': cust['id'], 'status': 'active', 'limit': 5}).get('data', [])
            if not subs:
                subs = _stripe_get('/v1/subscriptions', {
                    'customer': cust['id'], 'status': 'trialing', 'limit': 5}).get('data', [])
            if subs:
                sub   = subs[0]
                item  = (sub.get('items', {}).get('data') or [{}])[0]
                price = item.get('price', {}) or {}
                nick  = (price.get('nickname') or '').lower()
                plan  = 'pro'
                for p in ('basic', 'pro', 'max'):
                    if p in nick:
                        plan = p
                        break
                result = {
                    'active': True,
                    'plan': plan,
                    'current_period_end': sub.get('current_period_end', 0),
                }
                break
    except Exception as e:
        print(f'  [Stripe] check error for {email}: {e}')
        result = {'active': False, 'error': 'stripe check failed'}
    _stripe_cache[email] = (now, result)
    return result

def _telegram_enabled():
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)

def _telegram_send_detailed(text):
    """Send to Telegram; returns (ok, detail) with Telegram's real error text."""
    if not _telegram_enabled():
        return False, 'telegram not configured (missing env vars)'
    try:
        url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
        payload = json.dumps({
            'chat_id': TELEGRAM_CHAT_ID,
            'text': text,
            'parse_mode': 'HTML',
            'disable_web_page_preview': True
        }).encode()
        req = urllib.request.Request(url, data=payload,
                                     headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=10, context=ssl_ctx) as r:
            resp = json.loads(r.read())
        if resp.get('ok'):
            return True, 'sent'
        return False, resp.get('description', 'unknown telegram error')
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read())
            desc = body.get('description', str(e))
        except Exception:
            desc = str(e)
        print(f'  [Telegram] HTTP {e.code}: {desc}')
        return False, f'HTTP {e.code}: {desc}'
    except Exception as e:
        print(f'  [Telegram] send error: {e}')
        return False, str(e)

def _telegram_send(text):
    """Send a message to the configured Telegram channel. Never raises."""
    ok, detail = _telegram_send_detailed(text)
    if not ok and _telegram_enabled():
        print(f'  [Telegram] send failed: {detail}')
    return ok

TG_WATCH_ALERTS = os.environ.get('TELEGRAM_WATCH_ALERTS', 'true').lower() == 'true'
TG_WATCH_MIN    = float(os.environ.get('TELEGRAM_WATCH_MIN', '65'))

def _telegram_signal_alert(sig, min_conf, tier='STRONG'):
    """Push a signal to the channel. tier: STRONG (auto-traded) or WATCH (heads-up).
    Returns 'sent', 'cooldown', or 'failed'."""
    sym = sig['symbol']
    now = time.time()
    if now - _tg_last_alert.get(sym, 0) < TG_COOLDOWN_SECS:
        return 'cooldown'
    side   = 'LONG 🟢' if sig['direction'] == 1 else 'SHORT 🔴'
    arrow  = '▲' if sig['direction'] == 1 else '▼'
    coin   = sym.replace('USDT', '')
    pat    = sig.get('pattern', 'None')
    pat_ln = f"\n🕯 Pattern: <b>{pat}</b>" if pat and pat != 'None' else ''
    if tier == 'STRONG':
        head = f"⚡ <b>STRONG SIGNAL</b> — {arrow} <b>{coin}</b>"
        foot = "🤖 AlgoRhythm v4 · auto-trade tier · not financial advice"
    else:
        head = f"👀 <b>WATCH</b> — {arrow} <b>{coin}</b>"
        foot = "🤖 AlgoRhythm v4 · watchlist tier (not auto-traded) · not financial advice"
    msg = (
        f"{head}\n"
        f"\n"
        f"📊 Direction: <b>{side}</b>\n"
        f"🎯 Confidence: <b>{sig['score']:.0f}%</b>\n"
        f"💵 Price: <b>${sig['price']}</b>\n"
        f"📈 ADX: {sig.get('adx', 0):.0f} · RSI: {sig.get('rsi', 50):.0f}{pat_ln}\n"
        f"\n"
        f"{foot}"
    )
    if _telegram_send(msg):
        _tg_last_alert[sym] = now
        print(f'  [Telegram] 📨 {tier} alert sent: {sym} {side} {sig["score"]:.0f}%')
        return 'sent'
    return 'failed' 

def _auto_trading_loop():
    """Main autonomous trading loop — runs every 5 minutes on Render"""
    import random
    # Stagger start to not hammer API immediately
    time.sleep(30)
    print('  [AutoTrader] 🤖 Autonomous trading engine started')
    print(f"  [AutoTrader] ⚙ CONFIG: AUTO_TRADING env={os.environ.get('AUTO_TRADING','<unset>')} | runtime auto_enabled={_auto_creds['auto_enabled']} | telegram={'ON' if _telegram_enabled() else 'OFF'} | build=v3.4-tiered-alerts")
    scan_interval = int(os.environ.get('SCAN_INTERVAL_SECS', '300'))  # 5 min default

    while True:
        try:
            _req_ctx.demo = _auto_creds.get('demo')  # route this cycle demo/live
            # Scan runs if auto-trading is ON *or* Telegram alerts are configured
            if not _auto_creds['auto_enabled'] and not _telegram_enabled():
                time.sleep(30)
                continue
            if _auto_creds['auto_enabled'] and (not _auto_creds['api_key'] or not _auto_creds['api_secret']):
                time.sleep(60)
                continue

            total_syms = len(SCAN_SYMBOLS) + len(SPOT_SCAN_SYMBOLS)
            print(f'  [AutoTrader] 🔍 Scanning {total_syms} symbols ({len(SCAN_SYMBOLS)} futures + {len(SPOT_SCAN_SYMBOLS)} spot)...')

            # Regime check — display only, does NOT block any direction
            regime=_check_regime()
            # Both LONG and SHORT always allowed regardless of regime
            # Each coin's technical analysis determines its own direction
            allowed_dirs=[1,-1]
            print(f'  [AutoTrader] 🌡️ Regime: {regime} | Trading: BOTH directions | Mode: {_auto_creds["trading_mode"]}')

            # Get current positions (signals-only mode may have no creds yet)
            try:
                positions=_get_positions() if (_auto_creds['api_key'] and _auto_creds['api_secret']) else []
            except Exception:
                positions=[]
            # Position capacity only blocks TRADING — never signal alerts.
            at_capacity = len(positions) >= 20
            if at_capacity and not _telegram_enabled():
                print(f'  [AutoTrader] Max 20 server positions reached ({len(positions)} open) — skipping scan')
                time.sleep(scan_interval)
                continue

            # Scan all symbols — USDT futures + spot pairs
            signals=[]
            all_scan = [(s,'linear') for s in SCAN_SYMBOLS] + [(s,'spot') for s in SPOT_SCAN_SYMBOLS]
            for sym, cat in all_scan:
                if sym in positions:
                    continue  # already in position
                result=_scan_symbol(sym, category=cat)
                if result and result['direction'] in allowed_dirs:
                    signals.append(result)
                time.sleep(0.25)  # rate limit

            if not signals:
                print(f'  [AutoTrader] No qualifying signals this scan (regime={regime})')
                # Show what came close
                print(f'  [AutoTrader] Scanned {len(all_scan)} symbols — all filtered out by: breakout/ADX/MA/RSI conditions')
                time.sleep(scan_interval)
                continue

            # Sort by score, take best
            signals.sort(key=lambda x: x['score'], reverse=True)
            min_conf=_auto_creds['min_confidence']
            strong=[s for s in signals if s['score']>=min_conf]

            if not strong:
                best=signals[0]
                print(f'  [AutoTrader] Best signal: {best["symbol"]} {best["score"]:.0f}% — below {min_conf}% threshold (need {min_conf}%+)')
                print(f'  [AutoTrader] Top 3: {[(s["symbol"],s["score"]) for s in signals[:3]]}')
                time.sleep(scan_interval)
                continue

            # 📨 Telegram: broadcast signals to the channel (both tiers)
            if _telegram_enabled():
                tg_sent = tg_cool = 0
                for sig in strong[:5]:
                    res = _telegram_signal_alert(sig, min_conf, 'STRONG')
                    tg_sent += (res == 'sent'); tg_cool += (res == 'cooldown')
                watch_sigs = []
                if TG_WATCH_ALERTS:
                    watch_sigs = [s for s in signals
                                  if TG_WATCH_MIN <= s['score'] < min_conf][:3]
                    for sig in watch_sigs:
                        res = _telegram_signal_alert(sig, min_conf, 'WATCH')
                        tg_sent += (res == 'sent'); tg_cool += (res == 'cooldown')
                print(f'  [Telegram] scan summary: {len(strong)} strong · {len(watch_sigs)} watch · {tg_sent} sent · {tg_cool} on cooldown')

            # Trading only happens when auto-trading is enabled
            if not _auto_creds['auto_enabled']:
                print(f'  [AutoTrader] Signals-only mode — {len(strong)} strong signal(s) alerted, no trades placed')
                time.sleep(scan_interval)
                continue

            # At position capacity: alerts sent above, but no new trades
            if at_capacity:
                print(f'  [AutoTrader] {len(strong)} signal(s) alerted — at max positions ({len(positions)}), no new trades')
                time.sleep(scan_interval)
                continue

            # Place trades for top signals
            trades_this_cycle=0
            for sig in strong[:5]:  # max 5 trades per scan
                if len(positions)+trades_this_cycle>=20: break
                sym=sig['symbol']
                side='Buy' if sig['direction']==1 else 'Sell'
                size=_auto_creds['trade_size']
                pat_str = f' | {sig["pattern"]}' if sig.get('pattern','None')!='None' else ''
                print(f'  [AutoTrader] 🔥 {sym} {side} {sig["score"]:.0f}% ADX={sig.get("adx",0):.0f} RSI={sig.get("rsi",50):.0f}{pat_str}')
                print(f'  [AutoTrader]    {sig.get("reason","")[:100]}')
                if _place_order(sym, side, size, sig['price'], category=sig.get('category','linear')):
                    trades_this_cycle+=1
                    time.sleep(1)

            print(f'  [AutoTrader] Cycle complete — {trades_this_cycle} trades placed. Next scan in {scan_interval//60}min')

        except Exception as e:
            print(f'  [AutoTrader] Loop error: {e}')

        time.sleep(scan_interval)

# Restore persisted auto_enabled — survives Render restarts
# User clicking SERVER OFF writes /tmp/auto_trading_state.txt
# We read it back here so restarts honour the user's last choice
try:
    with open('/tmp/auto_trading_state.txt', 'r') as _sf:
        _persisted = _sf.read().strip()
    if _persisted in ('true', 'false'):
        _auto_creds['auto_enabled'] = (_persisted == 'true')
        print(f"  [AutoTrader] Restored auto_enabled={_auto_creds['auto_enabled']} from saved state")
except FileNotFoundError:
    pass  # First run — use env var default (AUTO_TRADING)
except Exception as _ex:
    print(f"  [AutoTrader] Could not read saved state: {_ex}")

# Start autonomous trading engine
_auto_thread = threading.Thread(target=_auto_trading_loop, daemon=True)
_auto_thread.start()

# Start trade management engine (breakeven, trailing SL/TP)
_mgmt_thread = threading.Thread(target=_trade_management_loop, daemon=True)
_mgmt_thread.start()

server = HTTPServer(("0.0.0.0", PORT), BybitProxyHandler)
try:
    server.serve_forever()
except KeyboardInterrupt:
    print("\n  Proxy stopped.")
