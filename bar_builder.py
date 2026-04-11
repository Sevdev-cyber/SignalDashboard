"""
Jajcus Bar Builder — converts raw OHLCV to enriched DataFrame.

Computes same indicators as backtest:
  ATR(14), EMA(20), EMA(50), EMA(100), VWAP, RSI(14), cum_delta
"""

import numpy as np
import pandas as pd


def enrich_bars(df: pd.DataFrame) -> pd.DataFrame:
    """Add technical indicators to OHLCV DataFrame.
    
    Matches _compute_bar_indicators from nt_tick_file_feed_adapter.py exactly.
    """
    frame = df.copy()
    
    # Delta: estimate from OHLCV if not present (warmup bars lack tick data)
    if "delta" not in frame.columns or (frame["delta"].abs().sum() == 0):
        # Close-position method: where close sits in the bar range
        # close near high → buyers dominated, close near low → sellers dominated
        bar_range = frame["high"] - frame["low"]
        close_pos = (frame["close"] - frame["low"]) / bar_range.replace(0, np.nan)
        close_pos = close_pos.fillna(0.5)  # doji = neutral
        # delta_pct ranges from -1 (close=low) to +1 (close=high)
        delta_pct = (close_pos * 2 - 1)
        # Apply estimation only to bars that have exactly 0.0 delta
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

    # Cumulative delta
    if "cum_delta" not in frame.columns:
        frame["cum_delta"] = frame["delta"].cumsum()

    # VWAP — uses Typical Price = (H+L+C)/3 per industry standard
    typical_price = (frame["high"] + frame["low"] + frame["close"]) / 3
    if "trade_value" not in frame.columns:
        frame["trade_value"] = typical_price * frame["volume"]
    cum_vol = frame["volume"].replace(0, np.nan).cumsum()
    frame["vwap"] = (frame["trade_value"].cumsum() / cum_vol).ffill().fillna(frame["close"])

    # EMAs
    frame["ema_20"] = frame["close"].ewm(span=20, adjust=False).mean()
    frame["ema_50"] = frame["close"].ewm(span=50, adjust=False).mean()
    frame["ema_100"] = frame["close"].ewm(span=100, adjust=False).mean()

    # RSI(14) — Wilder's smoothed EMA (industry standard)
    close_diff = frame["close"].diff()
    gain = close_diff.clip(lower=0.0).ewm(alpha=1/14, adjust=False).mean()
    loss = (-close_diff.clip(upper=0.0)).ewm(alpha=1/14, adjust=False).mean()
    rs = gain / loss.replace(0.0, np.nan)
    frame["rsi"] = (100 - 100 / (1 + rs)).fillna(50.0)

    # Session metadata
    frame["session"] = "jajcus_live"

    # Cleanup
    frame.drop(columns=["trade_value"], errors="ignore", inplace=True)

    return frame.reset_index(drop=True)


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


def append_bar(bars_df: pd.DataFrame, bar, true_delta: float = 0.0) -> pd.DataFrame:
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
    df = pd.concat([bars_df, pd.DataFrame([new_row])], ignore_index=True)
    
    # Re-enrich entire DataFrame (fast with 200 bars)
    return enrich_bars(df)
