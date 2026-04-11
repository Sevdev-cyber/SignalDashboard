"""
Signal Predictive Power Study — Event Study Approach
=====================================================
Dla każdego sygnału mierzymy CO ROBI CENA po jego wykryciu.
Zero SL/TP. Zero decyzji tradingowych. Czysta predykcyjność.

Dla każdego sygnału rejestrujemy:
- Cenę w momencie wykrycia
- Ścieżkę ceny przez następne 1, 2, 5, 10, 15, 30, 60, 120 minut
- Max Favorable Excursion (MFE) w każdym oknie
- Max Adverse Excursion (MAE) w każdym oknie
- Czy sygnał "miał rację" (cena poszła w przewidywanym kierunku)

Wynik: parquet z jednym wierszem na sygnał × okno czasowe.
"""

import os
import sys
import time
import glob
import logging
import bisect
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
from tqdm import tqdm

# Path setup
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))
sys.path.append("/Users/sacredforest/Trading Setup/SignalDashboard")

from signal_engine import SignalEngine

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger("signal_study")

# Okna czasowe do analizy (w minutach)
FORWARD_WINDOWS = [1, 2, 5, 10, 15, 30, 60, 120]

TICK_SIZE = 0.25


def discover_data_files(data_dir: str) -> list[str]:
    """Znajdź wszystkie pliki tick data (.csv.gz) rekurencyjnie."""
    files = glob.glob(os.path.join(data_dir, "**/*.csv.gz"), recursive=True)
    # Dodaj pliki bezpośrednio w katalogu
    files += glob.glob(os.path.join(data_dir, "*.csv.gz"))
    # Deduplikacja i sort
    files = sorted(set(files))
    return files


def load_ticks(filepath: str) -> pd.DataFrame:
    """Ładuj ticki z pliku CSV.GZ. Obsługuje format Nautilus L2."""
    try:
        parts = pd.read_csv(filepath, sep=';', nrows=2)
        if 'timestamp' in parts.columns and 'price' in parts.columns:
            df = pd.read_csv(filepath, sep=';', usecols=['timestamp', 'price', 'volume'])
            if df['timestamp'].dtype in ['int64', 'float64'] and df['timestamp'].iloc[0] > 1e15:
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ns')
            else:
                df['timestamp'] = pd.to_datetime(df['timestamp'], exact=False)
        else:
            raise ValueError("No headers")
    except Exception:
        try:
            df = pd.read_csv(filepath, sep=';', names=["timestamp_str", "price", "volume"])
        except Exception:
            df = pd.read_csv(filepath, sep=',', names=["timestamp_str", "price", "volume"])
        df['timestamp'] = pd.to_datetime(df['timestamp_str'], exact=False, errors='coerce')
        df.dropna(subset=['timestamp'], inplace=True)

    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
    df.dropna(subset=['price'], inplace=True)
    df.sort_values("timestamp", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def resample_to_bars(df_ticks: pd.DataFrame, timeframe_min: int = 5) -> pd.DataFrame:
    df_idx = df_ticks.set_index('timestamp')
    bars = df_idx['price'].resample(f'{timeframe_min}min').agg(['first', 'max', 'min', 'last'])
    bars.columns = ['open', 'high', 'low', 'close']
    vol = df_idx['volume'].resample(f'{timeframe_min}min').sum()
    bars['volume'] = vol
    bars.dropna(inplace=True)
    bars.reset_index(inplace=True)
    bars['tr'] = bars['high'] - bars['low']
    bars['atr'] = bars['tr'].rolling(14).mean().bfill()
    return bars


def compute_forward_path(
    tick_times: np.ndarray,
    tick_prices: np.ndarray,
    signal_time: pd.Timestamp,
    signal_price: float,
    direction: str,
    windows_min: list[int],
) -> dict:
    """
    Dla danego sygnału oblicz ścieżkę cenową w przód.
    Zwraca dict z metrykami per okno czasowe.
    """
    result = {}
    dir_sign = 1.0 if direction == "long" else -1.0

    # Konwertuj na numpy datetime64 dla porównań
    signal_time_np = np.datetime64(signal_time, 'ns')

    # Znajdź indeks startu (pierwszy tick >= signal_time)
    start_idx = np.searchsorted(tick_times, signal_time_np, side='left')
    if start_idx >= len(tick_times):
        return None  # Brak danych forward

    for w_min in windows_min:
        end_time_np = signal_time_np + np.timedelta64(w_min, 'm')
        end_idx = np.searchsorted(tick_times, end_time_np, side='right')

        # Wycinek cen w oknie [start, end)
        window_prices = tick_prices[start_idx:end_idx]

        if len(window_prices) == 0:
            # Brak danych w tym oknie — oznaczamy jako NaN
            result[f"move_{w_min}m"] = np.nan
            result[f"mfe_{w_min}m"] = np.nan
            result[f"mae_{w_min}m"] = np.nan
            result[f"end_price_{w_min}m"] = np.nan
            result[f"n_ticks_{w_min}m"] = 0
            continue

        # Ruchy cenowe względem ceny sygnału (w punktach)
        moves = (window_prices - signal_price) * dir_sign

        end_price = float(window_prices[-1])
        end_move = (end_price - signal_price) * dir_sign

        # MFE = max ruch w "dobrym" kierunku (pozytywna wartość = korzystny)
        mfe = float(np.max(moves))
        # MAE = max ruch w "złym" kierunku (pozytywna wartość = niekorzystny)
        mae = float(-np.min(moves)) if np.min(moves) < 0 else 0.0

        result[f"move_{w_min}m"] = round(end_move, 2)
        result[f"mfe_{w_min}m"] = round(mfe, 2)
        result[f"mae_{w_min}m"] = round(mae, 2)
        result[f"end_price_{w_min}m"] = round(end_price, 2)
        result[f"n_ticks_{w_min}m"] = len(window_prices)

    return result


def run_study(data_dir: str, output_path: str):
    log.info("=" * 60)
    log.info("  SIGNAL PREDICTIVE POWER STUDY")
    log.info("  Czysta predykcyjność — zero SL/TP")
    log.info("=" * 60)

    all_files = discover_data_files(data_dir)
    log.info(f"Znaleziono {len(all_files)} plików tick data")

    engine = SignalEngine()
    global_bars = pd.DataFrame()
    all_signals = []
    t0 = time.time()

    for file_idx, filepath in enumerate(all_files):
        fname = os.path.basename(filepath)
        log.info(f"[{file_idx+1}/{len(all_files)}] {fname}")

        df_ticks = load_ticks(filepath)
        if df_ticks.empty or len(df_ticks) < 100:
            log.warning(f"  Pomijam {fname} — za mało danych ({len(df_ticks)} ticks)")
            continue

        # Normalizuj timezone — usuń tz jeśli jest
        if df_ticks['timestamp'].dt.tz is not None:
            df_ticks['timestamp'] = df_ticks['timestamp'].dt.tz_localize(None)

        # Przygotuj tablice numpy do szybkiego lookup
        tick_times = df_ticks['timestamp'].values.astype('datetime64[ns]')
        tick_prices = df_ticks['price'].values.astype(np.float64)

        # Buduj bary
        df_bars = resample_to_bars(df_ticks, timeframe_min=5)
        if len(df_bars) < 15:
            continue

        # Doklej do historii (zachowaj ostatnie 200 barów)
        if not global_bars.empty:
            global_bars = pd.concat([global_bars, df_bars], ignore_index=True).tail(200)
        else:
            global_bars = df_bars

        # Przeskanuj bary z tego dnia
        day_start_idx = max(0, len(global_bars) - len(df_bars))

        for bar_idx in range(day_start_idx, len(global_bars)):
            history_slice = global_bars.iloc[:bar_idx + 1]
            if len(history_slice) < 15:
                continue

            bar_time = global_bars.iloc[bar_idx]['timestamp']
            bar_close = float(global_bars.iloc[bar_idx]['close'])

            try:
                candidates = engine.evaluate(
                    history_slice,
                    current_price=bar_close,
                    now=pd.Timestamp(bar_time).to_pydatetime(),
                )
            except Exception:
                continue

            for cand in candidates:
                if not isinstance(cand, dict):
                    continue

                conf = cand.get('confidence_pct', 0)
                if conf < 30:  # Zbieramy nawet słabe sygnały do analizy
                    continue

                signal_time = pd.Timestamp(bar_time)
                signal_price = bar_close

                # Oblicz ścieżkę forward
                fwd = compute_forward_path(
                    tick_times, tick_prices,
                    signal_time, signal_price,
                    cand['direction'], FORWARD_WINDOWS,
                )

                if fwd is None:
                    continue

                # Zbuduj rekord
                record = {
                    'signal_name': cand.get('name', 'UNKNOWN'),
                    'direction': cand['direction'],
                    'confidence': conf,
                    'score': cand.get('score', 0),
                    'regime': cand.get('regime', 'unknown'),
                    'regime_match': cand.get('regime_match', False),
                    'confluence_count': cand.get('confluence_count', 0),
                    'confluences': '|'.join(cand.get('reasons', [])),
                    'time_edge': cand.get('time_edge', ''),
                    'day_edge': cand.get('day_edge', ''),
                    'entry_suggested': cand.get('entry', 0),
                    'signal_time': str(signal_time),
                    'signal_price': signal_price,
                    'atr': cand.get('atr', 0),
                    'file': fname,
                }
                record.update(fwd)
                all_signals.append(record)

        # Zrzut co 20 plików
        if (file_idx + 1) % 20 == 0 and all_signals:
            log.info(f"  Checkpoint: {len(all_signals)} sygnałów zebranych")

    # Zapisz wynik
    if all_signals:
        df_out = pd.DataFrame(all_signals)
        df_out.to_parquet(output_path, index=False)
        elapsed = time.time() - t0
        log.info("=" * 60)
        log.info(f"ZAKOŃCZONO w {elapsed:.0f}s")
        log.info(f"Sygnałów zbadanych: {len(df_out):,}")
        log.info(f"Zapisano: {output_path}")

        # Quick summary
        for sig_name in df_out['signal_name'].unique():
            sub = df_out[df_out['signal_name'] == sig_name]
            n = len(sub)
            if n < 3:
                continue
            correct_5m = (sub['move_5m'].dropna() > 0).mean() * 100
            correct_30m = (sub['move_30m'].dropna() > 0).mean() * 100
            avg_mfe_5m = sub['mfe_5m'].dropna().mean()
            avg_mfe_30m = sub['mfe_30m'].dropna().mean()
            log.info(
                f"  {sig_name:20s} | N={n:5d} | "
                f"Correct@5m={correct_5m:5.1f}% | Correct@30m={correct_30m:5.1f}% | "
                f"MFE@5m={avg_mfe_5m:6.1f}pt | MFE@30m={avg_mfe_30m:7.1f}pt"
            )
    else:
        log.warning("Brak sygnałów do zapisania!")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python signal_study.py <data_directory> <output.parquet>")
        print("  data_directory: katalog z plikami .csv.gz (tick data)")
        print("  output.parquet: ścieżka do pliku wynikowego")
        sys.exit(1)

    run_study(sys.argv[1], sys.argv[2])
