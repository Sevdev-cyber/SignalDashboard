"""TCP writer — sends order commands to TickStreamer.

Thread-safe writer that shares the socket with TcpTickReader.
"""

from __future__ import annotations

import logging
import threading

log = logging.getLogger(__name__)


class TcpOrderWriter:
    """Sends order commands to TickStreamer via TCP.

    Commands follow the TickStreamer ASCII protocol.
    """

    def __init__(self, reader, *, dry_run: bool = True) -> None:
        self._reader = reader
        self.dry_run = dry_run
        self._lock = threading.Lock()

    def send(self, command: str) -> bool:
        if self.dry_run:
            log.info("[DRY RUN] Would send: %s", command)
            return True
        if not self._reader.connected or self._reader._sock is None:
            log.error("Cannot send — not connected")
            return False
        with self._lock:
            try:
                self._reader._sock.sendall((command + "\n").encode("utf-8"))
                log.info("Sent: %s", command)
                return True
            except OSError as e:
                log.error("Send failed: %s", e)
                return False

    # --- High-level order commands ---
    # Protocol matches TickStreamerMirror ProcessCommand() exactly:
    # Separator: ';'
    # BUY;qty;signal        SELL;qty;signal
    # BUY_LIMIT;qty;price;signal;oco   SELL_LIMIT;qty;price;signal;oco
    # BUY_STOP;qty;price;signal;oco    SELL_STOP;qty;price;signal;oco
    # CANCEL;signal    CLOSE    PING

    def buy_market(self, qty: int = 1, signal_name: str = "") -> bool:
        if signal_name:
            return self.send(f"BUY;{qty};{signal_name}")
        return self.send(f"BUY;{qty}")

    def sell_market(self, qty: int = 1, signal_name: str = "") -> bool:
        if signal_name:
            return self.send(f"SELL;{qty};{signal_name}")
        return self.send(f"SELL;{qty}")

    def buy_limit(self, qty: int, price: float, signal_name: str = "", oco: str = "") -> bool:
        return self.send(f"BUY_LIMIT;{qty};{price:.2f};{signal_name};{oco}")

    def sell_limit(self, qty: int, price: float, signal_name: str = "", oco: str = "") -> bool:
        return self.send(f"SELL_LIMIT;{qty};{price:.2f};{signal_name};{oco}")

    def buy_stop(self, qty: int, price: float, signal_name: str = "", oco: str = "") -> bool:
        return self.send(f"BUY_STOP;{qty};{price:.2f};{signal_name};{oco}")

    def sell_stop(self, qty: int, price: float, signal_name: str = "", oco: str = "") -> bool:
        return self.send(f"SELL_STOP;{qty};{price:.2f};{signal_name};{oco}")

    def cancel(self, signal_name: str) -> bool:
        return self.send(f"CANCEL;{signal_name}")

    def close_position(self) -> bool:
        return self.send("CLOSE")

    def ping(self) -> bool:
        return self.send("PING")
