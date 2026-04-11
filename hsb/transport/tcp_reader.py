"""TCP reader — connects to TickStreamer on NT8 and reads market data.

Cleaned up version of V1's vendor/scalper_v4_ultimate/live/tcp_reader.py.
Now part of the hsb package instead of being vendored.
"""

from __future__ import annotations

import logging
import socket
import threading
import time
from dataclasses import dataclass, field

log = logging.getLogger(__name__)


@dataclass
class BarUpdate:
    """Parsed bar data from TickStreamer."""

    timestamp: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    delta: float = 0.0
    is_closed: bool = True


@dataclass
class TickUpdate:
    """Parsed tick from TickStreamer."""

    timestamp: str
    price: float
    size: int
    aggressor: str  # BUY | SELL | UNKNOWN


@dataclass
class FillEvent:
    signal_name: str
    action: str
    qty: int
    price: float


class TcpTickReader:
    """Connects to TickStreamerMirror indicator on NT8 via TCP.

    Protocol (ASCII line-based, semicolon-separated):
    - ``B;ts;O;H;L;C;V;closed`` — bar warmup
    - ``BC;ts;O;H;L;C;V``       — bar close (live)
    - ``T;ts;price;size;aggressor;bid;ask`` — tick
    - ``H;ticks=N;bars=N;pos=X;time=T`` — heartbeat
    - ``FILL;signal;action;qty;price`` — order fill
    - ``REJECTED;signal;reason`` — order rejected
    - ``CANCELLED;signal`` — order cancelled
    - ``ACK;cmd;status;msg`` — command ack
    - ``ORDERSTATE;signal;state;filled;price;error`` — order state
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 5556,
        reconnect_delay: float = 2.0,
    ) -> None:
        self.host = host
        self.port = port
        self.reconnect_delay = reconnect_delay
        self._sock: socket.socket | None = None
        self._connected = threading.Event()
        self._stop = threading.Event()
        self._buffer = ""
        self._lock = threading.Lock()

        # Callback hooks
        self.on_bar: list = []
        self.on_bar_close: list = []
        self.on_tick: list = []
        self.on_fill: list = []
        self.on_rejected: list = []
        self.on_cancelled: list = []
        self.on_ack: list = []
        self.on_heartbeat: list = []
        self.on_orderstate: list = []

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    def connect(self, timeout: float = 10.0) -> bool:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect((self.host, self.port))
            sock.settimeout(0.5)
            self._sock = sock
            self._connected.set()
            log.info("Connected to TickStreamer %s:%d", self.host, self.port)
            return True
        except (OSError, ConnectionRefusedError) as e:
            log.warning("Connection failed %s:%d — %s", self.host, self.port, e)
            return False

    def disconnect(self) -> None:
        self._stop.set()
        self._connected.clear()
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    def read_loop(self) -> None:
        """Blocking read loop — call from a thread."""
        while not self._stop.is_set():
            if not self._connected.is_set():
                if not self.connect():
                    time.sleep(self.reconnect_delay)
                    continue

            try:
                data = self._sock.recv(4096)  # type: ignore[union-attr]
                if not data:
                    log.warning("TickStreamer: connection closed by remote")
                    self._connected.clear()
                    continue
                self._buffer += data.decode("utf-8", errors="replace")
                self._process_buffer()
            except socket.timeout:
                continue
            except OSError as e:
                log.warning("TickStreamer: connection lost: %s", e)
                self._connected.clear()
                time.sleep(self.reconnect_delay)

    def _process_buffer(self) -> None:
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            line = line.strip()
            if not line:
                continue
            self._dispatch(line)

    def _dispatch(self, line: str) -> None:
        # TickStreamerMirror uses ';' as separator
        parts = line.split(";")
        msg_type = parts[0]

        try:
            if msg_type in ("B", "BC"):
                bar = BarUpdate(
                    timestamp=parts[1],
                    open=float(parts[2]),
                    high=float(parts[3]),
                    low=float(parts[4]),
                    close=float(parts[5]),
                    volume=float(parts[6]) if len(parts) > 6 else 0.0,
                    buy_volume=float(parts[7]) if len(parts) > 7 else 0.0,
                    sell_volume=float(parts[8]) if len(parts) > 8 else 0.0,
                    delta=float(parts[9]) if len(parts) > 9 else 0.0,
                    is_closed=(msg_type == "BC"),
                )
                callbacks = self.on_bar_close if msg_type == "BC" else self.on_bar
                for cb in callbacks:
                    cb(bar)

            elif msg_type == "T" and len(parts) >= 5:
                tick = TickUpdate(
                    timestamp=parts[1],
                    price=float(parts[2]),
                    size=int(parts[3]),
                    aggressor=parts[4],
                )
                for cb in self.on_tick:
                    cb(tick)

            elif msg_type == "H":
                for cb in self.on_heartbeat:
                    cb()

            elif msg_type == "FILL" and len(parts) >= 5:
                fill = FillEvent(
                    signal_name=parts[1],
                    action=parts[2],
                    qty=int(parts[3]),
                    price=float(parts[4]),
                )
                for cb in self.on_fill:
                    cb(fill)

            elif msg_type == "REJECTED" and len(parts) >= 3:
                for cb in self.on_rejected:
                    cb(parts[1], parts[2])

            elif msg_type == "CANCELLED" and len(parts) >= 2:
                for cb in self.on_cancelled:
                    cb(parts[1])

            elif msg_type == "ACK" and len(parts) >= 3:
                for cb in self.on_ack:
                    cb(parts[1], parts[2])

            elif msg_type == "ORDERSTATE" and len(parts) >= 3:
                for cb in self.on_orderstate:
                    cb(parts[1], parts[2])

        except (ValueError, IndexError) as e:
            log.debug("Parse error on line %r: %s", line, e)
