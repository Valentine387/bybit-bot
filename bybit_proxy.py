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
    url   = BASE_URL + path + ("?" + query if query else "")
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
    url      = BASE_URL + path
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
            _auto_creds['api_key']       = body.get('api_key','')
            _auto_creds['api_secret']    = body.get('api_secret','')
            _auto_creds['trading_mode']  = body.get('trading_mode','linear')
            _auto_creds['trade_size']    = float(body.get('trade_size', 100))
            _auto_creds['tp_pct']        = float(body.get('tp_pct', 15))
            _auto_creds['sl_pct']        = float(body.get('sl_pct', 3))
            _auto_creds['min_confidence']= float(body.get('min_confidence', 75))
            enabled = body.get('auto_enabled', False)
            _auto_creds['auto_enabled']  = bool(enabled)
            print(f"  [AutoTrader] Credentials updated — auto={'ON' if enabled else 'OFF'} mode={_auto_creds['trading_mode']} size=${_auto_creds['trade_size']}")
            self.send_json(200, {"status": "ok", "auto_enabled": _auto_creds['auto_enabled']})

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
            })

        elif path == "/auto-toggle":
            # Toggle auto trading on/off
            _auto_creds['auto_enabled'] = not _auto_creds['auto_enabled']
            state = 'ON' if _auto_creds['auto_enabled'] else 'OFF'
            print(f"  [AutoTrader] Auto-trading toggled {state}")
            self.send_json(200, {"auto_enabled": _auto_creds['auto_enabled'], "status": state})
        elif path == "/test":
            # Test API keys with simplest possible Bybit call
            self.handle_test(api_key, api_secret)
        elif path == "/ws_status":
            self.send_json(200, {"subscribed": list(ws_subscribed), "cache": list(price_cache.keys())})
        else:
            self.send_json(404, {"error": "Not found"})

    def do_POST(self):
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
    'min_confidence': float(os.environ.get('MIN_CONFIDENCE', '75')),
}
_open_positions = {}  # symbol -> position data
_last_regime = 'UNKNOWN'

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
        url = BASE_URL + path
        if method == 'GET' and params:
            url += '?' + urllib.parse.urlencode(params)
        data = json.dumps(body).encode() if body else None
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f'  [AutoTrader] API error {path}: {e}')
        return None

def _get_klines(symbol, interval, limit=100):
    """Fetch candles from Bybit"""
    try:
        url = f'{BASE_URL}/v5/market/kline?symbol={symbol}&interval={interval}&limit={limit}&category={_auto_creds["trading_mode"]}'
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

def _scan_symbol(symbol):
    """Run AlgoRhythm 4-condition check on a symbol server-side"""
    try:
        c30m = _get_klines(symbol,'30',100)
        c4h  = _get_klines(symbol,'240',60)
        if not c30m or len(c30m)<30: return None
        closes=[c['close'] for c in c30m]
        n=len(closes)
        cur=closes[-1]
        rh=max(c['high'] for c in c30m[-22:-2])
        rl=min(c['low']  for c in c30m[-22:-2])
        up_break=cur>rh; dn_break=cur<rl
        if not up_break and not dn_break: return None
        direction=1 if up_break else -1
        adx=_calc_adx(c30m,14)
        if not adx or adx<20: return None
        ma20=_calc_ma(closes,20)
        if not ma20: return None
        if direction==1 and cur<ma20: return None
        if direction==-1 and cur>ma20: return None
        # MA votes
        sma50=_calc_ma(closes,50); ema_c=closes[-1]
        ma_v=0
        if direction==1:
            if cur>ma20: ma_v+=1
            if ma20 and sma50 and ma20>sma50: ma_v+=1
            if ma_v>=1: ma_v+=1  # simplified EMA proxy
        else:
            if cur<ma20: ma_v+=1
            if ma20 and sma50 and ma20<sma50: ma_v+=1
            if ma_v>=1: ma_v+=1
        if ma_v<3: return None
        rsi=_calc_rsi(closes,14)
        if rsi is None: return None
        if direction==1 and not (35<=rsi<=75): return None
        if direction==-1 and not (25<=rsi<=65): return None
        # H4 check (soft)
        h4_ok=True
        if c4h and len(c4h)>=20:
            h4cl=[c['close'] for c in c4h]
            h4ma=_calc_ma(h4cl,50)
            if h4ma:
                h4_ok=(direction==1 and h4cl[-1]>h4ma) or (direction==-1 and h4cl[-1]<h4ma)
        # Score
        score=40
        if up_break or dn_break: score+=15
        if adx>=25: score+=12
        elif adx>=20: score+=8
        if ma_v>=3: score+=10
        if h4_ok: score+=8
        rsi_ok=(direction==1 and rsi<60) or (direction==-1 and rsi>40)
        if rsi_ok: score+=10
        score=min(100,score)
        return {'symbol':symbol,'direction':direction,'score':score,'adx':adx,'rsi':rsi,'price':cur}
    except Exception as e:
        return None

SCAN_SYMBOLS = [
    'BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','DOGEUSDT','BNBUSDT',
    'ADAUSDT','AVAXUSDT','LINKUSDT','DOTUSDT','UNIUSDT','LTCUSDT',
    'NEARUSDT','INJUSDT','ARBUSDT','OPUSDT','SUIUSDT','APTUSDT',
    'TONUSDT','HYPEUSDT','PEPEUSDT','SHIBUSDT','WIFUSDT','BONKUSDT',
    'TRXUSDT','XLMUSDT','ATOMUSDT','FILUSDT','RENDERUSDT','FETUSDT',
]

def _get_positions():
    """Fetch open positions from Bybit"""
    global _open_positions
    try:
        r = _bybit_request('GET','/v5/position/list',{'category':_auto_creds['trading_mode'],'settleCoin':'USDT'})
        if r and r.get('retCode')==0:
            positions={}
            for p in r.get('result',{}).get('list',[]):
                if float(p.get('size',0))>0:
                    positions[p['symbol']]=p
            _open_positions=positions
            return positions
    except: pass
    return _open_positions

def _place_order(symbol, side, size, price):
    """Place a market order on Bybit"""
    try:
        mode=_auto_creds['trading_mode']
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
        if mode=='linear': body['leverage']='1'
        r=_bybit_request('POST','/v5/order/create',body=body)
        if r and r.get('retCode')==0:
            print(f'  [AutoTrader] ✅ {side} {symbol}: {qty} units @ ~${price:.4f} (${qty*price:.2f})')
            return True
        else:
            print(f'  [AutoTrader] ❌ Order failed {symbol}: {r}')
            return False
    except Exception as e:
        print(f'  [AutoTrader] Order error {symbol}: {e}')
        return False

def _auto_trading_loop():
    """Main autonomous trading loop — runs every 5 minutes on Render"""
    import random
    # Stagger start to not hammer API immediately
    time.sleep(30)
    print('  [AutoTrader] 🤖 Autonomous trading engine started')
    scan_interval = int(os.environ.get('SCAN_INTERVAL_SECS', '300'))  # 5 min default

    while True:
        try:
            if not _auto_creds['auto_enabled']:
                time.sleep(30)
                continue
            if not _auto_creds['api_key'] or not _auto_creds['api_secret']:
                time.sleep(60)
                continue

            print(f'  [AutoTrader] 🔍 Scanning {len(SCAN_SYMBOLS)} symbols...')

            # Check market regime
            regime=_check_regime()
            if regime=='RANGING':
                print(f'  [AutoTrader] ↔️ Market ranging — skipping this scan')
                time.sleep(scan_interval)
                continue

            allowed_dirs=[1] if regime=='TRENDING_UP' else ([-1] if regime=='TRENDING_DOWN' else [1,-1])

            # Get current positions
            positions=_get_positions()
            if len(positions)>=6:
                print(f'  [AutoTrader] Max positions reached ({len(positions)}) — skipping')
                time.sleep(scan_interval)
                continue

            # Scan all symbols
            signals=[]
            for sym in SCAN_SYMBOLS:
                if sym in positions:
                    continue  # already in position
                result=_scan_symbol(sym)
                if result and result['direction'] in allowed_dirs:
                    signals.append(result)
                time.sleep(0.3)  # rate limit

            if not signals:
                print(f'  [AutoTrader] No qualifying signals this scan (regime={regime})')
                time.sleep(scan_interval)
                continue

            # Sort by score, take best
            signals.sort(key=lambda x: x['score'], reverse=True)
            min_conf=_auto_creds['min_confidence']
            strong=[s for s in signals if s['score']>=min_conf]

            if not strong:
                best=signals[0]
                print(f'  [AutoTrader] Best: {best["symbol"]} {best["score"]:.0f}% — below {min_conf}% threshold')
                time.sleep(scan_interval)
                continue

            # Place trades for top signals
            trades_this_cycle=0
            for sig in strong[:3]:  # max 3 trades per scan
                if len(positions)+trades_this_cycle>=6: break
                sym=sig['symbol']
                side='Buy' if sig['direction']==1 else 'Sell'
                size=_auto_creds['trade_size']
                print(f'  [AutoTrader] 🔥 {sym} {side} {sig["score"]:.0f}% ADX={sig["adx"]:.0f} RSI={sig["rsi"]:.0f}')
                if _place_order(sym, side, size, sig['price']):
                    trades_this_cycle+=1
                    time.sleep(1)

            print(f'  [AutoTrader] Cycle complete — {trades_this_cycle} trades placed. Next scan in {scan_interval//60}min')

        except Exception as e:
            print(f'  [AutoTrader] Loop error: {e}')

        time.sleep(scan_interval)

# Start autonomous engine in background thread
_auto_thread = threading.Thread(target=_auto_trading_loop, daemon=True)
_auto_thread.start()

server = HTTPServer(("0.0.0.0", PORT), BybitProxyHandler)
try:
    server.serve_forever()
except KeyboardInterrupt:
    print("\n  Proxy stopped.")
