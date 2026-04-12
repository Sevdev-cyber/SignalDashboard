"""Wizjoner ↔ Bookmap Bridge Addon

Connects Bookmap to Wizjoner signal server:
1. RECEIVES signals from Wizjoner (WebSocket) → draws on heatmap + places orders
2. SENDS orderbook data from Bookmap → Wizjoner for enhanced signal generation

Architecture:
  Bookmap (Mac/VPS) ←WebSocket→ Wizjoner signal_server (VPS)
  Bookmap ←Rithmic→ CME (execution)

Setup in Bookmap:
  1. API Addon → Add → select this file
  2. Enable on MNQ chart
  3. Configure Wizjoner WS URL in settings
"""

import json
import threading
import time
from dataclasses import dataclass, field

import bookmap as bm

# ── CONFIG ──
DEFAULT_WS_URL = "wss://web-production-3ff3f.up.railway.app/ws"  # Railway relay (works from anywhere)
SIGNAL_CHECK_INTERVAL = 1.0  # seconds between signal polls
MAX_ACTIVE_ORDERS = 2  # max simultaneous orders
COMMAND_PORT = 9900  # local TCP port for LLM/script commands
COMMAND_SECRET = "sacred"  # simple auth token


@dataclass
class SignalState:
    """Tracks a signal from detection to order fill."""
    signal_id: str = ""
    direction: str = ""  # "long" or "short"
    entry: float = 0.0
    sl: float = 0.0
    tp1: float = 0.0
    confidence: int = 0
    grade: str = ""
    tier: str = ""
    order_id: str = None  # Bookmap order ID once placed
    filled: bool = False
    sl_order_id: str = None
    tp_order_id: str = None


# ── GLOBAL STATE ──
addon = None
alias_to_info = {}
alias_to_order_book = {}
active_signals: dict[str, SignalState] = {}
ws_connected = False
latest_signals = []
latest_state = {}
req_id_counter = 0
ws_url = DEFAULT_WS_URL

# Indicators
ind_signal_line = {}  # alias → indicator ID for signal entry level
ind_tp_line = {}
ind_sl_line = {}


def next_req_id():
    global req_id_counter
    req_id_counter += 1
    return req_id_counter


# ═══════════════════════════════════════════════════════════
# BOOKMAP EVENT HANDLERS
# ═══════════════════════════════════════════════════════════

def handle_subscribe(addon_obj, alias, full_name, is_crypto, pips,
                     size_mult, instr_mult, features):
    """Called when user enables addon on an instrument."""
    alias_to_info[alias] = {
        "alias": alias, "full_name": full_name,
        "pips": pips, "size_mult": size_mult, "instr_mult": instr_mult,
    }
    alias_to_order_book[alias] = bm.create_order_book()

    # Subscribe to market data
    bm.subscribe_to_depth(addon_obj, alias, next_req_id())
    bm.subscribe_to_trades(addon_obj, alias, next_req_id())
    bm.subscribe_to_order_info(addon_obj, alias, next_req_id())
    bm.subscribe_to_position_updates(addon_obj, alias, next_req_id())

    # Register custom indicators on heatmap
    ind_signal_line[alias] = bm.register_indicator(
        addon_obj, alias, "Wizjoner Entry",
        graph_type=bm.IndicatorGraphType.PRIMARY,
        color=(255, 215, 0), line_style=bm.LineStyle.SOLID, initial_value=0)

    ind_tp_line[alias] = bm.register_indicator(
        addon_obj, alias, "Wizjoner TP",
        graph_type=bm.IndicatorGraphType.PRIMARY,
        color=(16, 185, 129), line_style=bm.LineStyle.DASHED, initial_value=0)

    ind_sl_line[alias] = bm.register_indicator(
        addon_obj, alias, "Wizjoner SL",
        graph_type=bm.IndicatorGraphType.PRIMARY,
        color=(239, 68, 68), line_style=bm.LineStyle.DASHED, initial_value=0)

    # Settings panel
    bm.add_string_settings_parameter(addon_obj, alias, "WS URL", DEFAULT_WS_URL)
    bm.add_boolean_settings_parameter(addon_obj, alias, "Auto-Trade", False)
    bm.add_number_settings_parameter(addon_obj, alias, "Contracts", 1, 1, 10, 1)
    bm.add_number_settings_parameter(addon_obj, alias, "Min Confidence", 70, 30, 100, 5)

    print(f"[Wizjoner] Subscribed to {alias}", flush=True)

    # Start WebSocket listener thread
    threading.Thread(target=ws_listener_thread, args=(alias,), daemon=True).start()


def handle_unsubscribe(addon_obj, alias):
    """Called when user disables addon."""
    global ws_connected
    ws_connected = False
    if alias in alias_to_order_book:
        del alias_to_order_book[alias]
    print(f"[Wizjoner] Unsubscribed from {alias}", flush=True)


def handle_depth(addon_obj, alias, is_bid, price, size):
    """Orderbook update — update local book."""
    if alias in alias_to_order_book:
        bm.on_depth(alias_to_order_book[alias], is_bid, price, size)


def handle_trade(addon_obj, alias, price, size, is_otc,
                 is_bid, is_execution_start, is_execution_end,
                 aggressor_order_id, passive_order_id):
    """Trade tick — can forward to Wizjoner for enhanced delta."""
    pass  # TODO: forward to Wizjoner if needed


def handle_order_update(addon_obj, event):
    """Order status update from Rithmic."""
    status = event.get("status", "")
    order_id = event.get("orderId", "")
    alias = event.get("instrumentAlias", "")

    if status == "FILLED":
        # Check if this is one of our signal orders
        for sid, sig in active_signals.items():
            if sig.order_id == order_id:
                sig.filled = True
                print(f"[Wizjoner] FILLED: {sig.direction} @ {sig.entry}", flush=True)
                # Place SL and TP orders
                place_bracket_orders(addon_obj, alias, sig)
                break

    elif status == "CANCELLED":
        for sid, sig in list(active_signals.items()):
            if sig.order_id == order_id:
                del active_signals[sid]
                print(f"[Wizjoner] Order cancelled: {sid}", flush=True)
                break

    print(f"[Wizjoner] Order update: {event}", flush=True)


def handle_position_update(addon_obj, event):
    """Position change from Rithmic."""
    print(f"[Wizjoner] Position: {event}", flush=True)


def handle_settings_change(addon_obj, alias, name, field_type, value):
    """User changed settings in Bookmap GUI."""
    global ws_url
    if name == "WS URL":
        ws_url = str(value)
        print(f"[Wizjoner] WS URL changed to: {ws_url}", flush=True)
    print(f"[Wizjoner] Setting: {name} = {value}", flush=True)


# ═══════════════════════════════════════════════════════════
# SIGNAL PROCESSING (from Wizjoner WebSocket)
# ═══════════════════════════════════════════════════════════

def on_interval(addon_obj, alias):
    """Called every 0.1s by Bookmap. Process commands + signals."""
    # Process queued commands from TCP socket
    process_commands(addon_obj)

    if not latest_signals:
        return

    instrument = alias_to_info.get(alias)
    if not instrument:
        return

    pips = instrument["pips"]

    # Update indicator lines for best signal
    if latest_signals:
        best = latest_signals[0]
        entry_level = int(best.get("entry", 0) / pips)
        tp_level = int(best.get("tp1", 0) / pips)
        sl_level = int(best.get("sl", 0) / pips)

        if alias in ind_signal_line:
            bm.add_point(addon_obj, alias, ind_signal_line[alias], entry_level)
        if alias in ind_tp_line:
            bm.add_point(addon_obj, alias, ind_tp_line[alias], tp_level)
        if alias in ind_sl_line:
            bm.add_point(addon_obj, alias, ind_sl_line[alias], sl_level)

    # Auto-trade: place orders for high-confidence signals
    # (only if Auto-Trade is enabled in settings)
    for sig_data in latest_signals[:3]:
        sid = sig_data.get("id", "")
        if sid in active_signals:
            continue  # already tracking

        conf = sig_data.get("confidence_pct", 0)
        grade = sig_data.get("quality_grade", "")
        tier = sig_data.get("tier_label", "")
        direction = sig_data.get("direction", "")

        # Only trade high-quality signals
        if conf < 70 or grade in ("D", "C"):
            continue

        if len(active_signals) >= MAX_ACTIVE_ORDERS:
            continue

        sig = SignalState(
            signal_id=sid,
            direction=direction,
            entry=sig_data.get("entry", 0),
            sl=sig_data.get("sl", 0),
            tp1=sig_data.get("tp1", 0),
            confidence=conf,
            grade=grade,
            tier=tier,
        )
        active_signals[sid] = sig

        # Place limit order at entry
        is_buy = direction == "long"
        order = bm.OrderSendParameters(alias, is_buy, 1)  # 1 contract
        order.limit_price = sig.entry
        order.client_id = f"wiz-{sid[:16]}"
        bm.send_order(addon_obj, order)
        sig.order_id = order.client_id

        arrow = "▲ LONG" if is_buy else "▼ SHORT"
        bm.send_user_message(addon_obj, alias,
                             f"[Wizjoner] {arrow} {sig_data.get('name','')} "
                             f"conf={conf}% grade={grade} tier={tier} "
                             f"entry={sig.entry:.2f} TP={sig.tp1:.2f} SL={sig.sl:.2f}")

        print(f"[Wizjoner] ORDER SENT: {arrow} @ {sig.entry} "
              f"(SL={sig.sl}, TP={sig.tp1})", flush=True)


def place_bracket_orders(addon_obj, alias, sig: SignalState):
    """Place SL and TP bracket orders after entry fill."""
    is_buy = sig.direction == "long"

    # TP: opposite direction limit
    tp_order = bm.OrderSendParameters(alias, not is_buy, 1)
    tp_order.limit_price = sig.tp1
    tp_order.client_id = f"wiz-tp-{sig.signal_id[:12]}"
    bm.send_order(addon_obj, tp_order)
    sig.tp_order_id = tp_order.client_id

    # SL: opposite direction stop
    sl_order = bm.OrderSendParameters(alias, not is_buy, 1)
    sl_order.stop_price = sig.sl
    sl_order.client_id = f"wiz-sl-{sig.signal_id[:12]}"
    bm.send_order(addon_obj, sl_order)
    sig.sl_order_id = sl_order.client_id

    print(f"[Wizjoner] Bracket placed: TP={sig.tp1:.2f} SL={sig.sl:.2f}", flush=True)


# ═══════════════════════════════════════════════════════════
# WEBSOCKET LISTENER (connects to Wizjoner signal_server)
# ═══════════════════════════════════════════════════════════

def ws_listener_thread(alias):
    """Background thread: connects to Wizjoner WS and receives signals."""
    global ws_connected, latest_signals, latest_state

    while True:
        try:
            import websocket
            print(f"[Wizjoner] Connecting to {ws_url}...", flush=True)
            ws = websocket.WebSocket()
            ws.connect(ws_url, timeout=10)
            ws_connected = True
            print(f"[Wizjoner] Connected!", flush=True)

            while ws_connected:
                try:
                    data = ws.recv()
                    if not data:
                        continue
                    msg = json.loads(data)

                    if "signals" in msg and msg["signals"]:
                        latest_signals = msg["signals"]

                    if "state" in msg:
                        latest_state = msg["state"]

                except Exception as e:
                    print(f"[Wizjoner] WS recv error: {e}", flush=True)
                    break

        except Exception as e:
            print(f"[Wizjoner] WS connect error: {e} — retrying in 5s", flush=True)

        ws_connected = False
        time.sleep(5)


# ═══════════════════════════════════════════════════════════
# COMMAND SOCKET (local TCP — for LLM, SSH, scripts)
# ═══════════════════════════════════════════════════════════
#
# Send JSON commands to localhost:9900 from anywhere:
#
#   # From terminal / SSH / LLM:
#   echo '{"auth":"sacred","cmd":"buy","price":24500,"sl":24480,"tp":24530,"qty":1}' | nc localhost 9900
#   echo '{"auth":"sacred","cmd":"sell","price":24500,"sl":24520,"tp":24470,"qty":2}' | nc localhost 9900
#   echo '{"auth":"sacred","cmd":"cancel_all"}' | nc localhost 9900
#   echo '{"auth":"sacred","cmd":"flatten"}' | nc localhost 9900
#   echo '{"auth":"sacred","cmd":"status"}' | nc localhost 9900
#
#   # From Python:
#   import socket, json
#   s = socket.socket(); s.connect(("localhost", 9900))
#   s.send(json.dumps({"auth":"sacred","cmd":"buy","price":24500,"sl":24480,"tp":24530}).encode())
#   print(s.recv(4096).decode()); s.close()
#
#   # Via SSH from VPS:
#   ssh user@mac 'echo "{\"auth\":\"sacred\",\"cmd\":\"buy\",\"price\":24500}" | nc localhost 9900'

command_queue = []  # thread-safe queue processed by on_interval


def command_server_thread():
    """TCP server accepting JSON commands on localhost."""
    import socket

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", COMMAND_PORT))
    srv.listen(5)
    srv.settimeout(1.0)
    print(f"[Wizjoner] Command socket listening on localhost:{COMMAND_PORT}", flush=True)

    while True:
        try:
            conn, addr = srv.accept()
            data = conn.recv(4096).decode("utf-8", errors="ignore").strip()
            if not data:
                conn.close()
                continue

            try:
                cmd = json.loads(data)
            except json.JSONDecodeError:
                conn.sendall(b'{"error":"invalid JSON"}\n')
                conn.close()
                continue

            # Auth check
            if cmd.get("auth") != COMMAND_SECRET:
                conn.sendall(b'{"error":"unauthorized"}\n')
                conn.close()
                continue

            # Queue command for processing in Bookmap thread (thread-safe)
            command_queue.append((cmd, conn))

        except socket.timeout:
            continue
        except Exception as e:
            print(f"[Wizjoner] Command socket error: {e}", flush=True)
            time.sleep(1)


def process_commands(addon_obj):
    """Process queued commands (called from on_interval in Bookmap thread)."""
    while command_queue:
        cmd, conn = command_queue.pop(0)
        try:
            result = execute_command(addon_obj, cmd)
            conn.sendall(json.dumps(result).encode() + b"\n")
        except Exception as e:
            conn.sendall(json.dumps({"error": str(e)}).encode() + b"\n")
        finally:
            try:
                conn.close()
            except:
                pass


def execute_command(addon_obj, cmd: dict) -> dict:
    """Execute a trading command."""
    action = cmd.get("cmd", "")

    # Find first subscribed alias (instrument)
    alias = next(iter(alias_to_info), None)
    if not alias and action not in ("status", "ping"):
        return {"error": "no instrument subscribed"}

    instrument = alias_to_info.get(alias, {})
    pips = instrument.get("pips", 0.25)

    if action == "ping":
        return {"ok": True, "msg": "pong", "active_signals": len(active_signals)}

    elif action == "status":
        return {
            "ok": True,
            "instrument": alias,
            "active_orders": len(active_signals),
            "signals": [
                {"id": s.signal_id, "dir": s.direction, "entry": s.entry,
                 "sl": s.sl, "tp": s.tp1, "filled": s.filled,
                 "conf": s.confidence, "grade": s.grade}
                for s in active_signals.values()
            ],
            "ws_connected": ws_connected,
            "latest_price": latest_state.get("price", 0),
        }

    elif action in ("buy", "sell"):
        price = cmd.get("price", 0)
        sl = cmd.get("sl", 0)
        tp = cmd.get("tp", 0)
        qty = cmd.get("qty", 1)
        order_type = cmd.get("type", "limit")  # "limit" or "market"

        if price <= 0 and order_type == "limit":
            return {"error": "price required for limit order"}

        is_buy = action == "buy"
        order = bm.OrderSendParameters(alias, is_buy, qty)

        if order_type == "market":
            # Market order — no price needed
            pass
        else:
            order.limit_price = price

        client_id = f"cmd-{action}-{int(time.time())}"
        order.client_id = client_id
        bm.send_order(addon_obj, order)

        # Track for bracket orders
        sig = SignalState(
            signal_id=client_id,
            direction="long" if is_buy else "short",
            entry=price,
            sl=sl,
            tp1=tp,
            confidence=100,
            grade="CMD",
            tier="MANUAL",
            order_id=client_id,
        )
        active_signals[client_id] = sig

        arrow = "▲ BUY" if is_buy else "▼ SELL"
        bm.send_user_message(addon_obj, alias,
                             f"[CMD] {arrow} {qty}x @ {price:.2f} "
                             f"SL={sl:.2f} TP={tp:.2f}")

        return {"ok": True, "order_id": client_id, "action": action,
                "price": price, "qty": qty, "sl": sl, "tp": tp}

    elif action == "cancel_all":
        cancelled = 0
        for sid, sig in list(active_signals.items()):
            if sig.order_id:
                try:
                    bm.cancel_order(addon_obj, alias, sig.order_id)
                    cancelled += 1
                except:
                    pass
            if sig.sl_order_id:
                try:
                    bm.cancel_order(addon_obj, alias, sig.sl_order_id)
                except:
                    pass
            if sig.tp_order_id:
                try:
                    bm.cancel_order(addon_obj, alias, sig.tp_order_id)
                except:
                    pass
        active_signals.clear()
        bm.send_user_message(addon_obj, alias, f"[CMD] Cancelled {cancelled} orders")
        return {"ok": True, "cancelled": cancelled}

    elif action == "flatten":
        # Cancel all orders + close position at market
        for sid, sig in list(active_signals.items()):
            for oid in [sig.order_id, sig.sl_order_id, sig.tp_order_id]:
                if oid:
                    try:
                        bm.cancel_order(addon_obj, alias, oid)
                    except:
                        pass
        active_signals.clear()

        # Send market order to flatten (requires knowing current position)
        bm.send_user_message(addon_obj, alias, "[CMD] Flatten — cancel all + close position")
        return {"ok": True, "msg": "all cancelled, flatten manually if position open"}

    elif action == "move_sl":
        # Move SL for a specific or all active signals
        new_sl = cmd.get("sl", 0)
        target_id = cmd.get("id", None)
        moved = 0
        for sid, sig in active_signals.items():
            if target_id and sid != target_id:
                continue
            if sig.sl_order_id:
                try:
                    bm.move_order(addon_obj, alias, sig.sl_order_id, new_sl)
                    sig.sl = new_sl
                    moved += 1
                except:
                    pass
        return {"ok": True, "moved": moved, "new_sl": new_sl}

    elif action == "move_tp":
        new_tp = cmd.get("tp", 0)
        target_id = cmd.get("id", None)
        moved = 0
        for sid, sig in active_signals.items():
            if target_id and sid != target_id:
                continue
            if sig.tp_order_id:
                try:
                    bm.move_order(addon_obj, alias, sig.tp_order_id, new_tp)
                    sig.tp1 = new_tp
                    moved += 1
                except:
                    pass
        return {"ok": True, "moved": moved, "new_tp": new_tp}

    else:
        return {"error": f"unknown command: {action}"}


# ═══════════════════════════════════════════════════════════
# MAIN — addon entry point
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    addon = bm.create_addon()

    # Register handlers
    bm.add_depth_handler(addon, handle_depth)
    bm.add_on_interval_handler(addon, on_interval)
    bm.add_on_order_updated_handler(addon, handle_order_update)
    bm.add_on_position_update_handler(addon, handle_position_update)
    bm.add_on_setting_change_handler(addon, handle_settings_change)
    bm.add_trades_handler(addon, handle_trade)

    # Start command socket (local TCP for LLM/scripts)
    threading.Thread(target=command_server_thread, daemon=True).start()

    # Start addon
    bm.start_addon(addon, handle_subscribe, handle_unsubscribe)

    # Block until addon is disabled
    bm.wait_until_addon_is_turned_off(addon)
