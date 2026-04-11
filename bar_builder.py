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

    This gives Bookmap-grade accuracy for delta, CVD, and VWAP:
    - Delta = actual buy_vol - sell_vol per bar (from aggressor field)
    - VWAP = sum(actual_trade_price * volume) per session
    - CVD = session-based cumulation of real deltas

    Args:
        bars_df: DataFrame with bars (already enriched, has 'datetime')
        warmup_ticks: list of LiveTick objects with .timestamp, .price, .size, .aggressor
    """
    if bars_df.empty or not warmup_ticks:
        return bars_df

    frame = bars_df.copy()

    # Parse tick timestamps and build per-bar aggregates
    # Bars are 5-min intervals — find which bar each tick belongs to
    bar_times = pd.to_datetime(frame["datetime"])
    bar_periods = []
    for i, bt in enumerate(bar_times):
        next_bt = bar_times.iloc[i + 1] if i + 1 < len(bar_times) else bt + pd.Timedelta(minutes=5)
        bar_periods.append((bt, next_bt, i))

    # Aggregate ticks per bar
    bar_buy = {}   # bar_idx -> buy volume
    bar_sell = {}  # bar_idx -> sell volume
    bar_tv = {}    # bar_idx -> sum(price * volume) for VWAP
    tick_count = 0

    for tick in warmup_ticks:
        try:
            tick_time = pd.to_datetime(tick.timestamp)
        except Exception:
            continue

        # Find matching bar (binary-ish: scan from end since most ticks are recent)
        bar_idx = None
        for bt, next_bt, idx in reversed(bar_periods):
            if bt <= tick_time < next_bt:
                bar_idx = idx
                break

        if bar_idx is None:
            continue

        tick_count += 1
        vol = tick.size

        if tick.aggressor == 1:  # buy
            bar_buy[bar_idx] = bar_buy.get(bar_idx, 0) + vol
        elif tick.aggressor == 2:  # sell
            bar_sell[bar_idx] = bar_sell.get(bar_idx, 0) + vol

        # Trade value for VWAP (real price * volume)
        bar_tv[bar_idx] = bar_tv.get(bar_idx, 0.0) + tick.price * vol

    # Apply real delta to bars that have tick data
    bars_with_ticks = set(bar_buy.keys()) | set(bar_sell.keys())
    real_count = 0

    for idx in bars_with_ticks:
        buy_v = bar_buy.get(idx, 0)
        sell_v = bar_sell.get(idx, 0)
        real_delta = buy_v - sell_v

        frame.at[idx, "delta"] = float(real_delta)
        frame.at[idx, "buy_volume"] = float(buy_v)
        frame.at[idx, "sell_volume"] = float(sell_v)
        real_count += 1

    # Apply real trade values for VWAP
    frame["tick_vwap_value"] = np.nan
    for idx, tv in bar_tv.items():
        frame.at[idx, "tick_vwap_value"] = tv
    # For bars without tick data, fall back to typical price
    mask_no_ticks = frame["tick_vwap_value"].isna()
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
