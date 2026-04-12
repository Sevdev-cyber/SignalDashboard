"""
Jajcus Bar Builder — converts raw OHLCV to enriched DataFrame.

Computes same indicators as backtest:
  ATR(14), EMA(20), EMA(50), EMA(100), VWAP, RSI(14), cum_delta

Delta/CVD follow Bookmap methodology:
  - Delta = buy_vol - sell_vol (raw contracts, from tick aggressor)
  - CVD = session-based cumulative delta (resets each trading day)
  - VWAP = sum(typical_price * volume) / sum(volume), session-based
"""

import logging
import numpy as np
import pandas as pd

log = logging.getLogger("jajcus.bar_builder")


def _session_groups(frame: pd.DataFrame):
    """CME futures session boundary: 18:00 ET (6 PM) each day.

    A bar at 17:59 belongs to the current session,
    a bar at 18:00 starts a NEW session.
    Returns a Series of session IDs for groupby.
    """
    if "datetime" not in frame.columns:
        return None
    dt = pd.to_datetime(frame["datetime"])
    # Shift by 6 hours so 18:00 → midnight boundary → date change
    shifted = dt - pd.Timedelta(hours=18)
    return shifted.dt.date


def enrich_bars(df: pd.DataFrame) -> pd.DataFrame:
    """Add technical indicators to OHLCV DataFrame."""
    frame = df.copy()

    # Delta: estimate from OHLCV for bars that lack tick data (delta==0)
    # Per-bar estimation — doesn't skip bars that already have real tick delta
    if "delta" not in frame.columns:
        frame["delta"] = 0.0
    mask = frame["delta"] == 0
    if mask.any():
        bar_range = frame["high"] - frame["low"]
        close_pos = (frame["close"] - frame["low"]) / bar_range.replace(0, np.nan)
        close_pos = close_pos.fillna(0.5)
        delta_pct = (close_pos * 2 - 1)
        frame.loc[mask, "delta"] = (delta_pct[mask] * frame["volume"][mask]).fillna(0)

    if "buy_volume" not in frame.columns:
        frame["buy_volume"] = frame.get("volume", pd.Series(0, index=frame.index)) * 0.5
    if "sell_volume" not in frame.columns:
        frame["sell_volume"] = frame.get("volume", pd.Series(0, index=frame.index)) * 0.5

    # ATR(14)
    prev_close = frame["close"].shift(1)
    tr = pd.concat([
        frame["high"] - frame["low"],
        (frame["high"] - prev_close).abs(),
        (frame["low"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    frame["atr"] = tr.rolling(14, min_periods=1).mean()

    # Session groups (CME: 18:00 ET boundary)
    session = _session_groups(frame)

    # Cumulative delta — session-based
    if session is not None:
        frame["cum_delta"] = frame.groupby(session)["delta"].cumsum()
    else:
        frame["cum_delta"] = frame["delta"].cumsum()

    # VWAP — session-based, typical price method
    typical_price = (frame["high"] + frame["low"] + frame["close"]) / 3
    trade_value = typical_price * frame["volume"]

    if session is not None:
        cum_tv = trade_value.groupby(session).cumsum()
        cum_vol = frame["volume"].groupby(session).cumsum().replace(0, np.nan)
    else:
        cum_tv = trade_value.cumsum()
        cum_vol = frame["volume"].replace(0, np.nan).cumsum()

    frame["vwap"] = (cum_tv / cum_vol).ffill().fillna(frame["close"])

    # EMAs
    frame["ema_20"] = frame["close"].ewm(span=20, adjust=False).mean()
    frame["ema_50"] = frame["close"].ewm(span=50, adjust=False).mean()
    frame["ema_100"] = frame["close"].ewm(span=100, adjust=False).mean()

    # RSI(14)
    close_diff = frame["close"].diff()
    gain = close_diff.clip(lower=0.0).ewm(alpha=1 / 14, adjust=False).mean()
    loss = (-close_diff.clip(upper=0.0)).ewm(alpha=1 / 14, adjust=False).mean()
    rs = gain / loss.replace(0.0, np.nan)
    frame["rsi"] = (100 - 100 / (1 + rs)).fillna(50.0)

    frame["session"] = "jajcus_live"

    return frame.reset_index(drop=True)


def apply_tick_deltas(bars_df: pd.DataFrame, warmup_ticks: list) -> pd.DataFrame:
    """Replace estimated delta with real tick-based delta from warmup ticks.

    Uses np.searchsorted for O(n log n) bar assignment.
    NT bar timestamps = CLOSE time, so tick at 10:02 belongs to bar closing at 10:05.
    """
    if bars_df.empty or not warmup_ticks:
        return bars_df

    frame = bars_df.copy()

    # Bar close times as int64 nanoseconds (bars are sorted chronologically)
    bar_dt = pd.to_datetime(frame["datetime"])
    # Strip timezone if present (both bar/tick should be same tz from NT)
    if hasattr(bar_dt.dt, 'tz') and bar_dt.dt.tz is not None:
        bar_dt = bar_dt.dt.tz_localize(None)
    bar_close_ns = bar_dt.values.astype(np.int64)

    # Collect raw tick data
    tick_times_raw = []
    tick_prices = []
    tick_sizes = []
    tick_aggrs = []
    for tick in warmup_ticks:
        tick_times_raw.append(tick.timestamp)
        tick_prices.append(tick.price)
        tick_sizes.append(tick.size)
        tick_aggrs.append(tick.aggressor)

    if not tick_times_raw:
        return bars_df

    # Batch-convert timestamps
    tick_dt = pd.to_datetime(tick_times_raw)
    if hasattr(tick_dt, 'tz') and tick_dt.tz is not None:
        tick_dt = tick_dt.tz_localize(None)
    tick_ns = tick_dt.values.astype(np.int64)

    # Debug: log time ranges to diagnose bar assignment issues
    log.info("Bar time range: %s → %s (%d bars)",
             bar_dt.iloc[0], bar_dt.iloc[-1], len(bar_dt))
    log.info("Tick time range: %s → %s (%d ticks)",
             tick_dt[0], tick_dt[-1], len(tick_dt))
    tick_prices = np.array(tick_prices, dtype=np.float64)
    tick_sizes = np.array(tick_sizes, dtype=np.int64)
    tick_aggrs = np.array(tick_aggrs, dtype=np.int32)

    # Bar assignment: tick belongs to bar whose CLOSE time is the first one >= tick time
    # searchsorted(side='left') returns index of first bar_close >= tick_time
    bar_indices = np.searchsorted(bar_close_ns, tick_ns, side='left')

    # Clip to valid range
    bar_indices = np.clip(bar_indices, 0, len(frame) - 1)

    tick_count = len(bar_indices)

    # Vectorized aggregation
    buy_mask = tick_aggrs == 1
    sell_mask = tick_aggrs == 2

    bar_buy = np.zeros(len(frame), dtype=np.int64)
    np.add.at(bar_buy, bar_indices[buy_mask], tick_sizes[buy_mask])

    bar_sell = np.zeros(len(frame), dtype=np.int64)
    np.add.at(bar_sell, bar_indices[sell_mask], tick_sizes[sell_mask])

    # Apply real delta only to bars with tick data
    has_ticks = (bar_buy + bar_sell) > 0
    real_count = int(has_ticks.sum())

    frame.loc[has_ticks, "delta"] = (bar_buy - bar_sell)[has_ticks].astype(float)
    frame.loc[has_ticks, "buy_volume"] = bar_buy[has_ticks].astype(float)
    frame.loc[has_ticks, "sell_volume"] = bar_sell[has_ticks].astype(float)

    # Distribution stats for debugging
    unique_bars = np.unique(bar_indices)
    log.info("Tick delta: %d ticks → %d/%d bars with real delta | bar range: [%d..%d]",
             tick_count, real_count, len(frame),
             int(unique_bars[0]) if len(unique_bars) > 0 else -1,
             int(unique_bars[-1]) if len(unique_bars) > 0 else -1)

    return enrich_bars(frame)


TARGET_TF_MIN = 5  # Dashboard always shows 5-min candles


def resample_to_5min(df: pd.DataFrame) -> pd.DataFrame:
    """Resample bars to 5-min candles.

    If bars are already 5-min (or coarser), returns as-is.
    If bars are finer (1-min, etc.), aggregates OHLCV+delta to 5-min.
    """
    if df.empty or len(df) < 2:
        return df

    dt = pd.to_datetime(df["datetime"])

    # Detect input bar interval (median diff between consecutive bars)
    diffs = dt.diff().dropna()
    if diffs.empty:
        return df
    median_sec = diffs.median().total_seconds()

    # If already >= 5 min, no resampling needed
    if median_sec >= TARGET_TF_MIN * 60 - 10:  # 10s tolerance
        log.info("Bars already %.0fs (~%.1fmin), no resample needed", median_sec, median_sec / 60)
        return df

    log.info("Resampling from %.0fs to %dmin bars (%d bars → ~%d)",
             median_sec, TARGET_TF_MIN, len(df),
             len(df) * median_sec / (TARGET_TF_MIN * 60))

    frame = df.copy()
    frame["datetime"] = dt
    frame = frame.set_index("datetime")

    # Use label='right' so the 5-min bar gets the CLOSE timestamp (matches NT8 convention)
    agg = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }
    if "delta" in frame.columns:
        agg["delta"] = "sum"
    if "buy_volume" in frame.columns:
        agg["buy_volume"] = "sum"
    if "sell_volume" in frame.columns:
        agg["sell_volume"] = "sum"

    resampled = frame.resample(f"{TARGET_TF_MIN}min", label="right", closed="right").agg(agg)
    resampled = resampled.dropna(subset=["open"])  # drop empty periods
    resampled = resampled.reset_index()

    log.info("Resampled: %d → %d bars (5-min)", len(df), len(resampled))
    return resampled


def warmup_bars_to_df(warmup_bars: list) -> pd.DataFrame:
    """Convert WarmupBar list to DataFrame."""
    if not warmup_bars:
        return pd.DataFrame()

    records = []
    for b in warmup_bars:
        records.append({
            "datetime": b.timestamp,
            "open": b.open,
            "high": b.high,
            "low": b.low,
            "close": b.close,
            "volume": b.volume,
            "delta": 0.0,
        })

    df = pd.DataFrame(records)
    df["datetime"] = pd.to_datetime(df["datetime"])

    # Resample to 5-min if needed (e.g. NT8 sends 1-min bars)
    df = resample_to_5min(df)

    return enrich_bars(df)


# ── Live bar accumulator for sub-5min inputs ──

class BarAccumulator:
    """Accumulates sub-5min bars into 5-min candles.

    If NT8 sends 1-min bars, this collects 5 of them before emitting
    a complete 5-min bar. Tracks OHLCV + delta within the window.
    """

    def __init__(self, target_min: int = TARGET_TF_MIN):
        self.target_sec = target_min * 60
        self._pending_open = None
        self._pending_high = -float("inf")
        self._pending_low = float("inf")
        self._pending_close = None
        self._pending_volume = 0.0
        self._pending_delta = 0.0
        self._pending_buy = 0.0
        self._pending_sell = 0.0
        self._window_start = None
        self._input_count = 0
        self._detected_interval = None

    def _detect_interval(self, bar_dt):
        """Auto-detect input bar interval on first bar."""
        if self._detected_interval is not None:
            return
        # Will be set properly after 2nd bar
        self._detected_interval = 0

    def add_bar(self, bar, true_delta: float = 0.0, buy_vol: float = 0.0,
                sell_vol: float = 0.0) -> dict | None:
        """Add a bar. Returns a completed 5-min bar dict, or None if still accumulating."""
        bar_dt = pd.to_datetime(bar.timestamp)

        # First bar: initialize window
        if self._window_start is None:
            # Align to 5-min boundary: floor to nearest 5-min
            minute = bar_dt.minute
            aligned_min = (minute // TARGET_TF_MIN) * TARGET_TF_MIN
            self._window_start = bar_dt.replace(minute=aligned_min, second=0, microsecond=0)
            self._pending_open = bar.open
            self._pending_high = bar.high
            self._pending_low = bar.low

        # Window end = start + 5 min
        window_end = self._window_start + pd.Timedelta(minutes=TARGET_TF_MIN)

        # Check if this bar belongs to NEXT window
        if bar_dt >= window_end and self._pending_open is not None:
            # Emit the completed 5-min bar
            completed = {
                "datetime": window_end,  # close time (NT8 convention)
                "open": self._pending_open,
                "high": self._pending_high,
                "low": self._pending_low,
                "close": self._pending_close,
                "volume": self._pending_volume,
                "delta": self._pending_delta,
                "buy_volume": self._pending_buy,
                "sell_volume": self._pending_sell,
            }

            # Reset for new window
            aligned_min = (bar_dt.minute // TARGET_TF_MIN) * TARGET_TF_MIN
            self._window_start = bar_dt.replace(minute=aligned_min, second=0, microsecond=0)
            self._pending_open = bar.open
            self._pending_high = bar.high
            self._pending_low = bar.low
            self._pending_close = bar.close
            self._pending_volume = bar.volume
            self._pending_delta = true_delta
            self._pending_buy = buy_vol
            self._pending_sell = sell_vol
            self._input_count += 1

            log.debug("5min bar emitted: O=%.2f H=%.2f L=%.2f C=%.2f V=%.0f",
                      completed["open"], completed["high"], completed["low"],
                      completed["close"], completed["volume"])
            return completed

        # Accumulate into current window
        self._pending_high = max(self._pending_high, bar.high)
        self._pending_low = min(self._pending_low, bar.low)
        self._pending_close = bar.close
        self._pending_volume += bar.volume
        self._pending_delta += true_delta
        self._pending_buy += buy_vol
        self._pending_sell += sell_vol
        self._input_count += 1
        return None


def append_bar(bars_df: pd.DataFrame, bar, true_delta: float = 0.0,
               **kwargs) -> pd.DataFrame:
    """Append a new bar and re-enrich.

    NOTE: For sub-5min inputs, use BarAccumulator in signal_server.py instead.
    This function is for direct 5-min bar appends.
    """
    new_row = {
        "datetime": pd.to_datetime(bar.timestamp),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
        "delta": true_delta,
    }
    df = pd.concat([bars_df, pd.DataFrame([new_row])], ignore_index=True)
    return enrich_bars(df)
