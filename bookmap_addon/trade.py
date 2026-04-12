#!/usr/bin/env python3
"""Send trading commands to NT8 TickStreamerMirror via TCP.

NT8 is already approved by Bulenox/Rithmic — no extra API needed.
Commands go: this script → TCP → NT8 → Rithmic → CME
Bookmap sees orders automatically (same Rithmic account).

Requirements:
  - NT8 running on VPS with TickStreamerMirror
  - AcceptCommands = true in TickStreamerMirror settings
  - TCP port open (default 5557)

Usage:
    python3 trade.py buy 24500                           # market buy 1 contract
    python3 trade.py buy_limit 24500 --qty 2             # limit buy 2 contracts @ 24500
    python3 trade.py sell_limit 24550 --qty 1             # limit sell 1 @ 24550
    python3 trade.py buy_limit 24500 --sl 24480 --tp 24530  # limit + bracket SL/TP
    python3 trade.py close                                # flatten position
    python3 trade.py cancel MyOrder123                    # cancel specific order
    python3 trade.py status                               # check connection

From LLM/Claude:
    import subprocess
    r = subprocess.run(["python3", "trade.py", "buy_limit", "24500", "--sl", "24480", "--tp", "24530"],
                       capture_output=True, text=True,
                       cwd="/Users/sacredforest/Trading Setup/SignalDashboard/bookmap_addon")
    print(r.stdout)

Via SSH:
    ssh sacredforest@mac 'python3 /path/to/trade.py buy_limit 24500 --sl 24480'
"""

import socket
import sys
import time
import argparse

# ── NT8 TickStreamerMirror connection ──
VPS_HOST = "66.42.117.137"
TCP_PORT = 5557
TIMEOUT = 5


def send_command(cmd: str) -> str:
    """Send TCP command to NT8 and read ACK response."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(TIMEOUT)
        s.connect((VPS_HOST, TCP_PORT))

        # NT8 protocol: each message is a line ending with newline
        s.sendall((cmd + "\n").encode("ascii"))

        # Read response (ACK;...)
        response = ""
        try:
            data = s.recv(4096).decode("ascii", errors="ignore")
            response = data.strip()
        except socket.timeout:
            response = "TIMEOUT (no ACK received)"

        s.close()
        return response
    except ConnectionRefusedError:
        return "ERROR: Connection refused — NT8 TickStreamerMirror not running or port blocked"
    except Exception as e:
        return f"ERROR: {e}"


def send_and_print(cmd: str, description: str = ""):
    """Send command and print result."""
    if description:
        print(f"→ {description}")
    print(f"  CMD: {cmd}")
    result = send_command(cmd)
    print(f"  ACK: {result}")
    return result


def main():
    if len(sys.argv) < 2:
        print("""
NT8 Trading Commands (via TickStreamerMirror TCP)
═══════════════════════════════════════════════

  Market orders:
    python3 trade.py buy                          # market buy 1 contract
    python3 trade.py sell                         # market sell 1 contract
    python3 trade.py buy --qty 3                  # market buy 3 contracts

  Limit orders:
    python3 trade.py buy_limit 24500              # limit buy @ 24500
    python3 trade.py sell_limit 24550 --qty 2     # limit sell 2x @ 24550

  Stop orders:
    python3 trade.py buy_stop 24550               # stop buy @ 24550
    python3 trade.py sell_stop 24480              # stop sell @ 24480

  Bracket (entry + SL + TP):
    python3 trade.py buy_limit 24500 --sl 24480 --tp 24530
    python3 trade.py sell_limit 24550 --sl 24570 --tp 24520

  Position management:
    python3 trade.py close                        # flatten all
    python3 trade.py cancel MyOrderName           # cancel specific order

  Status:
    python3 trade.py ping                         # check connection

  VPS: {VPS_HOST}:{TCP_PORT}
""")
        sys.exit(0)

    action = sys.argv[1].lower()

    parser = argparse.ArgumentParser()
    parser.add_argument("action")
    parser.add_argument("price", nargs="?", default="0")
    parser.add_argument("--qty", type=int, default=1)
    parser.add_argument("--sl", type=float, default=0)
    parser.add_argument("--tp", type=float, default=0)
    parser.add_argument("--name", type=str, default="")
    args = parser.parse_args()

    # Generate order name
    order_name = args.name or f"Claude_{action}_{int(time.time())}"
    oco_group = f"oco_{int(time.time())}" if args.sl or args.tp else ""

    if action == "ping":
        send_and_print("PING", "Checking NT8 connection")

    elif action == "buy":
        # Market buy
        send_and_print(f"BUY;{args.qty};{order_name}", f"Market BUY {args.qty}x")

    elif action == "sell":
        # Market sell
        send_and_print(f"SELL;{args.qty};{order_name}", f"Market SELL {args.qty}x")

    elif action == "buy_limit":
        price = float(args.price)
        send_and_print(
            f"BUY_LIMIT;{args.qty};{price};{order_name};{oco_group}",
            f"Limit BUY {args.qty}x @ {price}"
        )
        # Place bracket if SL/TP specified
        if args.sl > 0:
            sl_name = f"SL_{order_name}"
            send_and_print(
                f"SELL_STOP;{args.qty};{args.sl};{sl_name};{oco_group}",
                f"  └─ SL SELL_STOP @ {args.sl}"
            )
        if args.tp > 0:
            tp_name = f"TP_{order_name}"
            send_and_print(
                f"SELL_LIMIT;{args.qty};{args.tp};{tp_name};{oco_group}",
                f"  └─ TP SELL_LIMIT @ {args.tp}"
            )

    elif action == "sell_limit":
        price = float(args.price)
        send_and_print(
            f"SELL_LIMIT;{args.qty};{price};{order_name};{oco_group}",
            f"Limit SELL {args.qty}x @ {price}"
        )
        if args.sl > 0:
            sl_name = f"SL_{order_name}"
            send_and_print(
                f"BUY_STOP;{args.qty};{args.sl};{sl_name};{oco_group}",
                f"  └─ SL BUY_STOP @ {args.sl}"
            )
        if args.tp > 0:
            tp_name = f"TP_{order_name}"
            send_and_print(
                f"BUY_LIMIT;{args.qty};{args.tp};{tp_name};{oco_group}",
                f"  └─ TP BUY_LIMIT @ {args.tp}"
            )

    elif action == "buy_stop":
        price = float(args.price)
        send_and_print(
            f"BUY_STOP;{args.qty};{price};{order_name};{oco_group}",
            f"Stop BUY {args.qty}x @ {price}"
        )

    elif action == "sell_stop":
        price = float(args.price)
        send_and_print(
            f"SELL_STOP;{args.qty};{price};{order_name};{oco_group}",
            f"Stop SELL {args.qty}x @ {price}"
        )

    elif action == "close":
        send_and_print("CLOSE", "Flatten position")

    elif action == "cancel":
        name = args.price  # second arg = order name to cancel
        send_and_print(f"CANCEL;{name}", f"Cancel order: {name}")

    else:
        print(f"Unknown action: {action}")
        sys.exit(1)


if __name__ == "__main__":
    main()
