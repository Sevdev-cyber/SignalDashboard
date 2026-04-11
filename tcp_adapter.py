"""
Jajcus TCP Adapter — connects to TickStreamerMirror on NT8.

Handles:
  - Warmup bar reception (B;...)
  - Live tick reception (T;...)
  - Bar close events (BC;...)
  - Heartbeats (H;...)
  - Order fills / cancellations / rejections
  - Sending order commands (BUY/SELL/CLOSE/LIMIT/STOP/CANCEL)

Protocol uses ';' separator — matches TickStreamerMirror exactly.
"""

import logging
import socket
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

log = logging.getLogger("jajcus.tcp")


@dataclass
class WarmupBar:
    timestamp: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    is_closed: bool


@dataclass
class LiveTick:
    timestamp: str
    price: float
    size: int
    aggressor: int  # 0=unknown, 1=buy, 2=sell
    bid: float
    ask: float


@dataclass
class FillEvent:
    name: str
    action: str  # Buy, Sell, BuyToCover, SellShort
    qty: int
    price: float


class TickStreamerAdapter:
    """Full-duplex TCP adapter for TickStreamerMirror."""

    def __init__(self, host: str = "127.0.0.1", port: int = 5556, dry_run: bool = True):
        self.host = host
        self.port = port
        self.dry_run = dry_run
        self._sock: Optional[socket.socket] = None
        self._lock = threading.Lock()
        self._running = False

        # State
        self.connected = False
        self.warmup_done = False
        self.warmup_bars: list[WarmupBar] = []
        self.warmup_ticks: list[LiveTick] = []

        # Callbacks
        self.on_warmup_complete: Optional[Callable] = None
        self.on_bar_close: Optional[Callable] = None
        self.on_tick: Optional[Callable] = None
        self.on_heartbeat: Optional[Callable] = None
        self.on_fill: Optional[Callable] = None
        self.on_cancelled: Optional[Callable] = None
        self.on_rejected: Optional[Callable] = None
        self.on_order_state: Optional[Callable] = None

    # ── Connection ──

    def connect(self, timeout: float = 15.0) -> bool:
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock.settimeout(timeout)
            self._sock.connect((self.host, self.port))
            self._sock.settimeout(1.0)
            self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.connected = True
            log.info("Connected to TickStreamerMirror %s:%d", self.host, self.port)
            return True
        except Exception as e:
            log.error("Connection failed: %s", e)
            return False

    def disconnect(self):
        self._running = False
        self.connected = False
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass

    # ── Send commands ──

    def _send(self, cmd: str) -> bool:
        if self.dry_run and not cmd.startswith("PING"):
            log.info("[DRY] %s", cmd)
            return True
        if not self.connected or not self._sock:
            log.error("Cannot send '%s' — not connected", cmd)
            return False
        with self._lock:
            try:
                self._sock.sendall((cmd + "\n").encode("ascii"))
                log.info("SENT: %s", cmd)
                return True
            except OSError as e:
                log.error("Send failed: %s", e)
                return False

    def buy_market(self, qty: int = 1, signal: str = "") -> bool:
        if signal:
            return self._send(f"BUY;{qty};{signal}")
        return self._send(f"BUY;{qty}")

    def sell_market(self, qty: int = 1, signal: str = "") -> bool:
        if signal:
            return self._send(f"SELL;{qty};{signal}")
        return self._send(f"SELL;{qty}")

    def buy_limit(self, qty: int, price: float, signal: str = "", oco: str = "") -> bool:
        return self._send(f"BUY_LIMIT;{qty};{price:.2f};{signal};{oco}")

    def sell_limit(self, qty: int, price: float, signal: str = "", oco: str = "") -> bool:
        return self._send(f"SELL_LIMIT;{qty};{price:.2f};{signal};{oco}")

    def buy_stop(self, qty: int, price: float, signal: str = "", oco: str = "") -> bool:
        return self._send(f"BUY_STOP;{qty};{price:.2f};{signal};{oco}")

    def sell_stop(self, qty: int, price: float, signal: str = "", oco: str = "") -> bool:
        return self._send(f"SELL_STOP;{qty};{price:.2f};{signal};{oco}")

    def cancel(self, signal: str) -> bool:
        return self._send(f"CANCEL;{signal}")

    def close_position(self) -> bool:
        return self._send("CLOSE")

    def ping(self) -> bool:
        return self._send("PING")

    # ── Read loop (blocking — run in thread) ──

    def read_loop(self):
        """Main read loop — blocks, call from a dedicated thread."""
        self._running = True
        buf = ""
        in_bar_warmup = False
        in_tick_warmup = False

        while self._running and self.connected:
            try:
                data = self._sock.recv(65536)
                if not data:
                    log.warning("Connection closed by remote")
                    self.connected = False
                    break
                buf += data.decode("ascii", errors="replace")
            except socket.timeout:
                continue
            except OSError as e:
                log.warning("Connection lost: %s", e)
                self.connected = False
                break

            while "\n" in buf:
                line, buf = buf.split("\n", 1)
                line = line.strip()
                if not line:
                    continue

                parts = line.split(";")
                msg = parts[0]

                try:
                    # ── Warmup bars ──
                    if msg == "BARS_START":
                        in_bar_warmup = True
                        self.warmup_bars.clear()
                        continue

                    if msg == "BARS_END":
                        in_bar_warmup = False
                        log.info("Received %d warmup bars", len(self.warmup_bars))
                        if not in_tick_warmup:
                            self.warmup_done = True
                            if self.on_warmup_complete:
                                self.on_warmup_complete()
                        continue

                    if msg == "TICKS_START":
                        in_tick_warmup = True
                        self.warmup_ticks.clear()
                        continue

                    if msg == "TICKS_END":
                        in_tick_warmup = False
                        log.info("Received %d warmup ticks", len(self.warmup_ticks))
                        self.warmup_done = True
                        if self.on_warmup_complete:
                            self.on_warmup_complete()
                        continue

                    if msg == "B" and len(parts) >= 7:
                        bar = WarmupBar(
                            timestamp=parts[1],
                            open=float(parts[2]),
                            high=float(parts[3]),
                            low=float(parts[4]),
                            close=float(parts[5]),
                            volume=float(parts[6]),
                            is_closed=bool(int(parts[7])) if len(parts) > 7 else True,
                        )
                        if in_bar_warmup:
                            self.warmup_bars.append(bar)
                        continue

                    # ── Live bar close ──
                    if msg == "BC" and len(parts) >= 7:
                        bar = WarmupBar(
                            timestamp=parts[1],
                            open=float(parts[2]),
                            high=float(parts[3]),
                            low=float(parts[4]),
                            close=float(parts[5]),
                            volume=float(parts[6]),
                            is_closed=True,
                        )
                        if self.on_bar_close:
                            self.on_bar_close(bar)
                        continue

                    # ── Live tick ──
                    if msg == "T" and len(parts) >= 5:
                        tick = LiveTick(
                            timestamp=parts[1],
                            price=float(parts[2]),
                            size=int(parts[3]),
                            aggressor=int(parts[4]) if len(parts) > 4 else 0,
                            bid=float(parts[5]) if len(parts) > 5 else 0.0,
                            ask=float(parts[6]) if len(parts) > 6 else 0.0,
                        )
                        if in_tick_warmup:
                            self.warmup_ticks.append(tick)
                        elif self.on_tick:
                            self.on_tick(tick)
                        continue

                    # ── Heartbeat ──
                    if msg == "H":
                        if self.on_heartbeat:
                            meta = {}
                            for p in parts[1:]:
                                if "=" in p:
                                    k, v = p.split("=", 1)
                                    meta[k] = v
                            self.on_heartbeat(meta)
                        continue

                    # ── Order events ──
                    if msg == "FILL" and len(parts) >= 5:
                        fill = FillEvent(
                            name=parts[1],
                            action=parts[2],
                            qty=int(parts[3]),
                            price=float(parts[4]),
                        )
                        log.info("FILL: %s %s %dc @ %.2f", fill.name, fill.action, fill.qty, fill.price)
                        if self.on_fill:
                            self.on_fill(fill)
                        continue

                    if msg == "CANCELLED":
                        name = parts[1] if len(parts) > 1 else ""
                        log.info("CANCELLED: %s", name)
                        if self.on_cancelled:
                            self.on_cancelled(name)
                        continue

                    if msg == "REJECTED":
                        name = parts[1] if len(parts) > 1 else ""
                        reason = parts[2] if len(parts) > 2 else "unknown"
                        log.warning("REJECTED: %s — %s", name, reason)
                        if self.on_rejected:
                            self.on_rejected(name, reason)
                        continue

                    if msg == "ORDERSTATE" and len(parts) >= 3:
                        if self.on_order_state:
                            self.on_order_state({
                                "name": parts[1],
                                "state": parts[2],
                                "filled": int(parts[3]) if len(parts) > 3 else 0,
                                "price": float(parts[4]) if len(parts) > 4 else 0.0,
                                "error": parts[5] if len(parts) > 5 else "",
                            })
                        continue

                    # ACK — just log
                    if msg == "ACK":
                        log.debug("ACK: %s", line)
                        continue

                except (ValueError, IndexError) as e:
                    log.debug("Parse error on '%s': %s", line[:80], e)
