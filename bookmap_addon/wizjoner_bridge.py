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
DEFAULT_WS_URL = "ws://66.42.117.137:8082"  # Wizjoner WS on VPS
SIGNAL_CHECK_INTERVAL = 1.0  # seconds between signal polls
MAX_ACTIVE_ORDERS = 2  # max simultaneous orders


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
    """Called every 0.1s by Bookmap. Process new signals."""
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

    # Start addon
    bm.start_addon(addon, handle_subscribe, handle_unsubscribe)

    # Block until addon is disabled
    bm.wait_until_addon_is_turned_off(addon)
