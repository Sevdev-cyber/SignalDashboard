"""Wizjoner Simple — minimal Bookmap addon, no threads, no WebSocket.

Just draws indicator lines and listens on TCP for commands.
Start with this to verify Bookmap integration works.
"""
import json
import bookmap as bm

addon = None
alias_info = {}
req_id = 0
ind_entry = {}
ind_tp = {}
ind_sl = {}

def next_id():
    global req_id
    req_id += 1
    return req_id

def on_subscribe(addon_state, alias, full_name, is_crypto, pips,
                 size_mult, instr_mult, features):
    alias_info[alias] = {"pips": pips}
    print(f"[Wizjoner] Connected to {alias} pips={pips}", flush=True)

    # Register 3 indicator lines on heatmap
    ind_entry[alias] = next_id()
    bm.register_indicator(addon, alias, ind_entry[alias], "Entry",
                          graph_type="PRIMARY", color=(255, 215, 0),
                          line_style="SOLID", initial_value=0)

    ind_tp[alias] = next_id()
    bm.register_indicator(addon, alias, ind_tp[alias], "TP",
                          graph_type="PRIMARY", color=(16, 185, 129),
                          line_style="DASHED", initial_value=0)

    ind_sl[alias] = next_id()
    bm.register_indicator(addon, alias, ind_sl[alias], "SL",
                          graph_type="PRIMARY", color=(239, 68, 68),
                          line_style="DASHED", initial_value=0)

    print("[Wizjoner] Indicators registered OK", flush=True)

def on_unsubscribe(addon_state, alias):
    print(f"[Wizjoner] Disconnected from {alias}", flush=True)

if __name__ == "__main__":
    addon = bm.create_addon()
    bm.start_addon(addon, on_subscribe, on_unsubscribe)
    print("[Wizjoner] Simple addon started", flush=True)
    bm.wait_until_addon_is_turned_off(addon)
