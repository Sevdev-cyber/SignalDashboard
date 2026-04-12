#!/usr/bin/env python3
"""Send trading commands to Bookmap Wizjoner Bridge.

Usage:
    python trade.py buy 24500 --sl 24480 --tp 24530 --qty 2
    python trade.py sell 24500 --sl 24520 --tp 24470
    python trade.py buy market --qty 1
    python trade.py status
    python trade.py cancel_all
    python trade.py flatten
    python trade.py move_sl 24485
    python trade.py move_tp 24540

From LLM/Claude:
    import subprocess
    result = subprocess.run(["python3", "trade.py", "buy", "24500", "--sl", "24480", "--tp", "24530"],
                            capture_output=True, text=True)
    print(result.stdout)

Via SSH from VPS:
    ssh user@mac 'cd /path/to/bookmap_addon && python3 trade.py buy 24500 --sl 24480 --tp 24530'
"""

import socket
import json
import sys
import argparse

HOST = "127.0.0.1"
PORT = 9900
SECRET = "sacred"


def send_command(cmd: dict) -> dict:
    cmd["auth"] = SECRET
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    try:
        s.connect((HOST, PORT))
        s.sendall(json.dumps(cmd).encode())
        response = s.recv(8192).decode()
        return json.loads(response)
    except ConnectionRefusedError:
        return {"error": "Bookmap addon not running (connection refused on port 9900)"}
    except Exception as e:
        return {"error": str(e)}
    finally:
        s.close()


def main():
    if len(sys.argv) < 2:
        print("Usage: python trade.py <command> [args]")
        print("Commands: buy, sell, status, cancel_all, flatten, move_sl, move_tp, ping")
        sys.exit(1)

    action = sys.argv[1].lower()

    if action == "ping":
        result = send_command({"cmd": "ping"})

    elif action == "status":
        result = send_command({"cmd": "status"})

    elif action == "cancel_all":
        result = send_command({"cmd": "cancel_all"})

    elif action == "flatten":
        result = send_command({"cmd": "flatten"})

    elif action in ("buy", "sell"):
        parser = argparse.ArgumentParser()
        parser.add_argument("action")
        parser.add_argument("price", nargs="?", default="0")
        parser.add_argument("--sl", type=float, default=0)
        parser.add_argument("--tp", type=float, default=0)
        parser.add_argument("--qty", type=int, default=1)
        args = parser.parse_args()

        order_type = "market" if args.price == "market" else "limit"
        price = 0 if order_type == "market" else float(args.price)

        result = send_command({
            "cmd": action,
            "price": price,
            "sl": args.sl,
            "tp": args.tp,
            "qty": args.qty,
            "type": order_type,
        })

    elif action == "move_sl":
        if len(sys.argv) < 3:
            print("Usage: python trade.py move_sl <new_price> [--id order_id]")
            sys.exit(1)
        new_sl = float(sys.argv[2])
        target_id = sys.argv[4] if len(sys.argv) > 4 and sys.argv[3] == "--id" else None
        result = send_command({"cmd": "move_sl", "sl": new_sl, "id": target_id})

    elif action == "move_tp":
        if len(sys.argv) < 3:
            print("Usage: python trade.py move_tp <new_price> [--id order_id]")
            sys.exit(1)
        new_tp = float(sys.argv[2])
        target_id = sys.argv[4] if len(sys.argv) > 4 and sys.argv[3] == "--id" else None
        result = send_command({"cmd": "move_tp", "tp": new_tp, "id": target_id})

    else:
        result = {"error": f"Unknown command: {action}"}

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
