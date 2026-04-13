#!/usr/bin/env python3
"""TradingView Webhook Receiver — receives bar context from PineScript.

Runs locally or on VPS. TradingView sends POST with JSON payload
on every bar close. Data is stored and available for LLM analysis.

Usage:
    python3 tv_webhook_receiver.py                    # port 9902
    python3 tv_webhook_receiver.py --port 9902

TradingView Alert Setup:
    1. On chart with LLM Context Reader indicator
    2. Create Alert → Condition: "LLM Context Reader" → "Bar Close Context"
    3. Check "Webhook URL" → enter: http://your-ip:9902/hook
    4. Message: paste the webhook_json from the indicator tooltip
    5. Set to "Once Per Bar Close"

For Railway/public URL — use the relay:
    Webhook URL: https://web-production-3ff3f.up.railway.app/tv-hook

LLM reads context:
    curl http://localhost:9902/latest | python3 -m json.tool
    # Or from Python:
    import requests; ctx = requests.get("http://localhost:9902/latest").json()
"""
import json
import logging
import os
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger("tv_hook")

PORT = int(os.environ.get("TV_WEBHOOK_PORT", 9902))
latest_context = {}
context_history = []
MAX_HISTORY = 500


class WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        global latest_context
        content_len = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_len).decode('utf-8', errors='ignore')

        try:
            data = json.loads(body)
            data["received_at"] = datetime.now().isoformat()
            latest_context = data
            context_history.append(data)
            if len(context_history) > MAX_HISTORY:
                context_history.pop(0)

            log.info("BAR %s | %s %.2f | %s %s | ATR=%.1f RSI=%.0f | FVG=%d↑%d↓",
                     data.get("time", "?"),
                     data.get("ticker", "?"),
                     data.get("price", 0),
                     data.get("trend", "?"),
                     data.get("vwap_pos", "?"),
                     data.get("atr", 0),
                     data.get("rsi", 0),
                     data.get("bull_fvg", 0),
                     data.get("bear_fvg", 0))

            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"ok":true}')
        except json.JSONDecodeError:
            log.warning("Bad JSON: %s", body[:100])
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'{"error":"bad json"}')

    def do_GET(self):
        if self.path == "/latest":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(latest_context, indent=2).encode())

        elif self.path.startswith("/history"):
            n = 20
            try:
                n = int(self.path.split("?n=")[1])
            except:
                pass
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(context_history[-n:], indent=2).encode())

        elif self.path == "/summary":
            # LLM-friendly text summary
            ctx = latest_context
            if not ctx:
                text = "No data received yet."
            else:
                text = f"""MARKET CONTEXT — {ctx.get('ticker','?')} {ctx.get('tf','')} @ {ctx.get('time','')}

Price: {ctx.get('price',0):.2f} (O={ctx.get('open',0):.2f} H={ctx.get('high',0):.2f} L={ctx.get('low',0):.2f})
Trend: {ctx.get('trend','?').upper()} | VWAP: {ctx.get('vwap_pos','?').upper()} ({ctx.get('vwap',0):.2f})
EMA20: {ctx.get('ema20',0):.2f} | EMA50: {ctx.get('ema50',0):.2f}
ATR14: {ctx.get('atr',0):.2f} | RSI14: {ctx.get('rsi',0):.1f} | Delta: {ctx.get('delta_pct',0):.1f}%
Session: {ctx.get('session','?')}

KEY LEVELS:
  Prev Week High: {ctx.get('pw_high',0):.2f} ({abs(ctx.get('price',0)-ctx.get('pw_high',0)):.1f} pts away)
  Prev Week Low:  {ctx.get('pw_low',0):.2f} ({abs(ctx.get('price',0)-ctx.get('pw_low',0)):.1f} pts away)
  Prev Day High:  {ctx.get('pd_high',0):.2f} ({abs(ctx.get('price',0)-ctx.get('pd_high',0)):.1f} pts away)
  Prev Day Low:   {ctx.get('pd_low',0):.2f} ({abs(ctx.get('price',0)-ctx.get('pd_low',0)):.1f} pts away)
  Prev Day Close: {ctx.get('pd_close',0):.2f}

FAIR VALUE GAPS:
  Bullish FVGs: {ctx.get('bull_fvg',0)}
  Bearish FVGs: {ctx.get('bear_fvg',0)}
  Nearest: {ctx.get('nearest_fvg_side','none')} @ {ctx.get('nearest_fvg_dist',0):.1f} pts
"""
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(text.encode())

        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"""<h3>TradingView Webhook Receiver</h3>
<ul>
<li><a href="/latest">/latest</a> — last bar context (JSON)</li>
<li><a href="/history?n=20">/history?n=20</a> — last N bars (JSON)</li>
<li><a href="/summary">/summary</a> — LLM-readable text summary</li>
</ul>
<p>POST /hook with TradingView webhook JSON</p>""")

    def log_message(self, format, *args):
        pass  # suppress default logging


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=PORT)
    args = parser.parse_args()

    server = HTTPServer(("0.0.0.0", args.port), WebhookHandler)
    log.info(f"TradingView webhook receiver on port {args.port}")
    log.info(f"  POST /hook     — receive TradingView webhook")
    log.info(f"  GET  /latest   — last bar context (JSON)")
    log.info(f"  GET  /summary  — LLM-readable text")
    log.info(f"  GET  /history  — bar history")
    server.serve_forever()
