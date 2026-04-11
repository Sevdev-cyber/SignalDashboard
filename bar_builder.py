"""
Jajcus Bar Builder — converts raw OHLCV to enriched DataFrame.

Computes same indicators as backtest:
  ATR(14), EMA(20), EMA(50), EMA(100), VWAP, RSI(14), cum_delta

Delta/CVD/VWAP follow Bookmap methodology:
  - Delta = buy_vol - sell_vol (raw contracts, from tick aggressor)
  - CVD = session-based cumulative delta (resets each trading day)
  - VWAP = sum(trade_price * volume) / sum(volume), session-based reset
"""

import logging
import numpy as np
import pandas as pd

log = logging.getLogger("jajcus.bar_builder")


def enrich_bars(df: pd.DataFrame) -> pd.DataFrame:
    """Add technical indicators to OHLCV DataFrame.

    Matches _compute_bar_indicators from nt_tick_file_feed_adapter.py exactly.
    """
    frame = df.copy()

    # Delta: estimate from OHLCV if not present (warmup bars lack tick data)
    if "delta" not in frame.columns:
        frame["delta"] = 0.0
    if frame["delta"].abs().sum() == 0:
        # Close-position method: where close sits in the bar range
        bar_range = frame["high"] - frame["low"]
        close_pos = (frame["close"] - frame["low"]) / bar_range.replace(0, np.nan)
        close_pos = close_pos.fillna(0.5)
        delta_pct = (close_pos * 2 - 1)
        mask = frame["delta"] == 0
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

    # Cumulative delta — session-based (Bookmap style: resets each trading day)
    if "datetime" in frame.columns:
        session_date = pd.to_datetime(frame["datetime"]).dt.date
        frame["cum_delta"] = frame.groupby(session_date)["delta"].cumsum()
    else:
        frame["cum_delta"] = frame["delta"].cumsum()

    # VWAP — session-based (Bookmap style: resets each trading day)
    # Uses tick_vwap_value if available (real trade prices), else typical price
    if "tick_vwap_value" in frame.columns:
        # Real tick-based VWAP: sum(price*vol) already computed from ticks
        trade_value = frame["tick_vwap_value"]
    else:
        typical_price = (frame["high"] + frame["low"] + frame["close"]) / 3
        trade_value = typical_price * frame["volume"]

    if "datetime" in frame.columns:
        session_date = pd.to_datetime(frame["datetime"]).dt.date
        cum_tv = trade_value.groupby(session_date).cumsum()
        cum_vol = frame["volume"].groupby(session_date).cumsum().replace(0, np.nan)
    else:
        cum_tv = trade_value.cumsum()
        cum_vol = frame["volume"].replace(0, np.nan).cumsum()

    frame["vwap"] = (cum_tv / cum_vol).ffill().fillna(frame["close"])

    # EMAs
    frame["ema_20"] = frame["close"].ewm(span=20, adjust=False).mean()
    frame["ema_50"] = frame["close"].ewm(span=50, adjust=False).mean()
    frame["ema_100"] = frame["close"].ewm(span=100, adjust=False).mean()

    # RSI(14) — Wilder's smoothed EMA (industry standard)
    close_diff = frame["close"].diff()
    gain = close_diff.clip(lower=0.0).ewm(alpha=1 / 14, adjust=False).mean()
    loss = (-close_diff.clip(upper=0.0)).ewm(alpha=1 / 14, adjust=False).mean()
    rs = gain / loss.replace(0.0, np.nan)
    frame["rsi"] = (100 - 100 / (1 + rs)).fillna(50.0)

    # Session metadata
    frame["session"] = "jajcus_live"

    # Cleanup
    frame.drop(columns=["trade_value", "tick_vwap_value"], errors="ignore", inplace=True)

    return frame.reset_index(drop=True)


def apply_tick_deltas(bars_df: pd.DataFrame, warmup_ticks: list) -> pd.DataFrame:
    """Replace estimated delta with real tick-based delta from warmup ticks.

    Uses numpy digitize for O(n log n) bar assignment instead of O(n×m) linear scan.
    43K ticks × 977 bars: ~0.1s instead of ~160s.

    Args:
        bars_df: DataFrame with bars (already enriched, has 'datetime')
        warmup_ticks: list of LiveTick objects with .timestamp, .price, .size, .aggressor
    """
    if bars_df.empty or not warmup_ticks:
        return bars_df

    frame = bars_df.copy()

    # Build bar edges as numpy int64 timestamps for fast digitize
    bar_times = pd.to_datetime(frame["datetime"])
    bar_edges = bar_times.astype(np.int64).values  # nanosecond timestamps

    # Parse all ticks into numpy arrays at once
    tick_times = []
    tick_prices = []
    tick_sizes = []
    tick_aggrs = []

    for tick in warmup_ticks:
        try:
            tt = pd.to_datetime(tick.timestamp).value  # nanosecond int64
        except Exception:
            continue
        tick_times.append(tt)
        tick_prices.append(tick.price)
        tick_sizes.append(tick.size)
        tick_aggrs.append(tick.aggressor)

    if not tick_times:
        return bars_df

    tick_times = np.array(tick_times, dtype=np.int64)
    tick_prices = np.array(tick_prices, dtype=np.float64)
    tick_sizes = np.array(tick_sizes, dtype=np.int64)
    tick_aggrs = np.array(tick_aggrs, dtype=np.int32)

    # Assign each tick to a bar using digitize: O(n log n)
    # digitize returns bin index where tick_time falls in bar_edges
    # bar_idx = digitize(tick_time, bar_edges) - 1 (bins are right-exclusive)
    bar_indices = np.digitize(tick_times, bar_edges) - 1
    # Clip to valid range
    bar_indices = np.clip(bar_indices, 0, len(frame) - 1)

    # Filter only ticks that fall within bar range
    valid = (bar_indices >= 0) & (bar_indices < len(frame))
    bar_indices = bar_indices[valid]
    tick_prices = tick_prices[valid]
    tick_sizes = tick_sizes[valid]
    tick_aggrs = tick_aggrs[valid]

    tick_count = len(bar_indices)

    # Vectorized aggregation per bar
    buy_mask = tick_aggrs == 1
    sell_mask = tick_aggrs == 2

    # Buy volume per bar
    bar_buy = np.zeros(len(frame), dtype=np.int64)
    np.add.at(bar_buy, bar_indices[buy_mask], tick_sizes[buy_mask])

    # Sell volume per bar
    bar_sell = np.zeros(len(frame), dtype=np.int64)
    np.add.at(bar_sell, bar_indices[sell_mask], tick_sizes[sell_mask])

    # Trade value per bar (for VWAP)
    bar_tv = np.zeros(len(frame), dtype=np.float64)
    np.add.at(bar_tv, bar_indices, tick_prices * tick_sizes)

    # Apply real delta only to bars that actually have tick data
    has_ticks = (bar_buy + bar_sell) > 0
    real_count = int(has_ticks.sum())

    frame.loc[has_ticks, "delta"] = (bar_buy - bar_sell)[has_ticks].astype(float)
    frame.loc[has_ticks, "buy_volume"] = bar_buy[has_ticks].astype(float)
    frame.loc[has_ticks, "sell_volume"] = bar_sell[has_ticks].astype(float)

    # Apply real trade values for VWAP (vectorized)
    frame["tick_vwap_value"] = np.where(has_ticks, bar_tv, np.nan)
    # For bars without tick data, fall back to typical price
    mask_no_ticks = ~has_ticks
    typical = (frame["high"] + frame["low"] + frame["close"]) / 3
    frame.loc[mask_no_ticks, "tick_vwap_value"] = (typical * frame["volume"])[mask_no_ticks]

    log.info("Tick delta: %d ticks → %d/%d bars with real delta",
             tick_count, real_count, len(frame))

    # Re-enrich to recalculate CVD, VWAP, etc. with real data
    return enrich_bars(frame)


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
    return enrich_bars(df)


def append_bar(bars_df: pd.DataFrame, bar, true_delta: float = 0.0,
               trade_value: float = 0.0) -> pd.DataFrame:
    """Append a new bar and re-enrich the last few rows."""
    new_row = {
        "datetime": pd.to_datetime(bar.timestamp),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
        "delta": true_delta,
    }
    # If we have real tick-based trade value, use it for VWAP
    if trade_value > 0:
        new_row["tick_vwap_value"] = trade_value

    df = pd.concat([bars_df, pd.DataFrame([new_row])], ignore_index=True)

    # Re-enrich entire DataFrame (fast with 200 bars)
    return enrich_bars(df)
