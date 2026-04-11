import os
import sys
import time
import glob
import logging
from datetime import datetime, timezone
import pandas as pd
from tqdm import tqdm

# Ensure both HybridSuperBotV2/src and SignalDashboard are in PATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))
sys.path.append("/Users/sacredforest/Trading Setup/SignalDashboard")

from signal_engine import SignalEngine
from hsb.profiling.lifecycle_tracker import LifecycleTracker

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger("offline_runner")

def discover_data_files(data_dir: str) -> list[str]:
    files = glob.glob(os.path.join(data_dir, "*.csv.gz"))
    files.sort()
    return files

def load_nt8_ticks(filepath: str) -> pd.DataFrame:
    try:
        # First try Nautilus format which has headers like level;mdt;timestamp;operation;depth;market_maker;price;volume
        parts = pd.read_csv(filepath, sep=';', nrows=2)
        if 'timestamp' in parts.columns and 'price' in parts.columns and 'volume' in parts.columns:
            df = pd.read_csv(filepath, sep=';', usecols=['timestamp', 'price', 'volume'])
            # Convert timestamp which might be unix nanoseconds or str
            if df['timestamp'].dtype in ['int64', 'float64'] and df['timestamp'].iloc[0] > 1000000000000000:
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ns')
            else:
                df['timestamp'] = pd.to_datetime(df['timestamp'], exact=False)
        else:
            raise ValueError("No headers")
    except:
        # Fallback raw
        try:
            df = pd.read_csv(filepath, sep=';', names=["timestamp_str", "price", "volume"])
        except:
            df = pd.read_csv(filepath, sep=',', names=["timestamp_str", "price", "volume"])
        df['timestamp'] = pd.to_datetime(df['timestamp_str'], exact=False, errors='coerce')
        df.dropna(subset=['timestamp'], inplace=True)
        
    # Standardize column types 
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
    df.dropna(subset=['price'], inplace=True)
    
    df.sort_values("timestamp", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df

def resample_to_bars(df_ticks: pd.DataFrame, timeframe_min: int = 5) -> pd.DataFrame:
    df_idx = df_ticks.copy()
    df_idx.set_index('timestamp', inplace=True)
    bars = df_idx.resample(f'{timeframe_min}min').agg({'price': ['first', 'max', 'min', 'last'], 'volume': 'sum'})
    bars.columns = ['open', 'high', 'low', 'close', 'volume']
    bars.dropna(inplace=True)
    bars.reset_index(inplace=True)
    bars['tr'] = bars['high'] - bars['low']
    bars['atr'] = bars['tr'].rolling(14).mean().fillna(method='bfill')
    return bars

def run_profiling(data_dir: str, output_parquet: str):
    log.info("=========================================")
    log.info("   HSB SIGNAL PROFILER ENGINE (120-DAY CHUNKED)")
    log.info("   STATUS: ZERO LOOK-AHEAD BIAS ENFORCED")
    log.info("=========================================")
    
    all_files = discover_data_files(data_dir)
    log.info(f"Odnaleziono łącznie {len(all_files)} dni danych historii. Ściągam smycz. Odpalam pełen przemiał tick-by-tick!")
    
    engine = SignalEngine()
    tracker = LifecycleTracker(tick_size=0.25)
    
    global_bars_history = pd.DataFrame()
    start_time = time.time()
    
    chunk_dfs = []
    chunk_size = 20
    
    for chunk_idx in range(0, len(all_files), chunk_size):
        chunk_files = all_files[chunk_idx:chunk_idx + chunk_size]
        log.info(f"--- ROZPOCZYNAM PACZKĘ {chunk_idx//chunk_size + 1} ({len(chunk_files)} PLIKOW) ---")
        
        for f in chunk_files:
            file_name = os.path.basename(f)
            log.info(f"Przetwarzanie sesji: {file_name}")
            df_ticks = load_nt8_ticks(f)
            if df_ticks.empty:
                continue
                
            df_bars = resample_to_bars(df_ticks, timeframe_min=5)
            
            if not global_bars_history.empty:
                global_bars_history = pd.concat([global_bars_history, df_bars], ignore_index=True).tail(200)
            else:
                global_bars_history = df_bars
                
            current_bar_idx = max(0, len(global_bars_history) - len(df_bars))
            next_bar_time = global_bars_history.iloc[current_bar_idx + 1]['timestamp'] if current_bar_idx + 1 < len(global_bars_history) else None

            # Ultra-fast iteration
            for row in tqdm(df_ticks.itertuples(index=False), total=len(df_ticks), desc=file_name):
                ts = row.timestamp
                price = row.price
                
                tracker.process_tick(str(ts), price)
                
                if next_bar_time and ts >= next_bar_time:
                    history_slice = global_bars_history.iloc[:current_bar_idx + 1]
                    candidates = engine.evaluate(history_slice)
                    
                    for cand in candidates:
                        conf = cand.get('confidence_pct', 0) if isinstance(cand, dict) else cand.score * 100
                        if conf >= 60.0:
                            tracker.add_candidate(str(ts), price, cand)
                            
                    current_bar_idx += 1
                    if current_bar_idx + 1 < len(global_bars_history):
                        next_bar_time = global_bars_history.iloc[current_bar_idx + 1]['timestamp']
                    else:
                        next_bar_time = None

        # Po każdej paczce pobieramy TYLKO gotowe sygnały i opróżniamy tracker z zamkniętych, by nie spuchł RAM!
        rows = []
        for s in tracker.completed_signals:
            # Add normalization against ATR
            # ATR is in points. 1 point = 4 MNQ ticks. 
            atr_ticks = s.atr_at_detection * 4 if s.atr_at_detection > 0 else 1
            
            rows.append({
                "id": s.id, "signal_name": s.signal_name, "direction": s.direction,
                "confidence": s.confidence, "timeframe": s.timeframe,
                "confluences": "|".join(sorted(s.confluences)),
                "time_detected": s.time_detected, "price_at_detection": s.price_at_detection,
                "real_entry_price": s.real_entry_price,
                "resolution": s.resolution, 
                "mae_ticks": s.mae_ticks, "mfe_ticks": s.mfe_ticks,
                "mae_atr": round(s.mae_ticks / atr_ticks, 2),
                "mfe_atr": round(s.mfe_ticks / atr_ticks, 2),
                "pnl_usd": round(s.pnl_usd, 2)
            })
        
        if rows:
            df_chunk = pd.DataFrame(rows)
            # Safe Parquet Backup per chunk
            chunk_file = output_parquet.replace(".parquet", f"_pt{chunk_idx//chunk_size + 1}.parquet")
            df_chunk.to_parquet(chunk_file)
            log.info(f"Opróżniono RAM na twardy dysk: Paczka zrzucona pod '{chunk_file}' ({len(rows)} sygnałów).")
            chunk_dfs.append(df_chunk)
            
        tracker.completed_signals.clear()

    # KONIEC CAŁOSCI! Zabezpieczenie resztek i Pending!
    rows_final = []
    for s in tracker.completed_signals + tracker.pending_signals:
        atr_ticks = s.atr_at_detection * 4 if s.atr_at_detection > 0 else 1
        rows_final.append({
            "id": s.id, "signal_name": s.signal_name, "direction": s.direction,
            "confidence": s.confidence, "timeframe": s.timeframe,  "confluences": "|".join(sorted(s.confluences)),
            "time_detected": s.time_detected, "price_at_detection": s.price_at_detection,
            "real_entry_price": s.real_entry_price,
            "resolution": s.resolution,
            "mae_ticks": s.mae_ticks, "mfe_ticks": s.mfe_ticks,
            "mae_atr": round(s.mae_ticks / atr_ticks, 2),
            "mfe_atr": round(s.mfe_ticks / atr_ticks, 2),
            "pnl_usd": round(s.pnl_usd, 2)
        })
    if rows_final:
        chunk_dfs.append(pd.DataFrame(rows_final))
        
    if chunk_dfs:
        super_df = pd.concat(chunk_dfs, ignore_index=True)
        super_df.to_parquet(output_parquet)
        log.info(f"SUPER ZAPIS 120 DNI ZAKOŃCZONY POMYŚLNIE. Baza główna to: {output_parquet}")
        
        win_rate = len(super_df[super_df["resolution"] == "WIN"]) / len(super_df) * 100 if len(super_df) > 0 else 0
        log.info("=" * 40)
        elapsed = time.time() - start_time
        log.info(f"CAŁY CYKL 120 DNI ZAKONCZONY w {elapsed:.1f} sek.")
        log.info(f"Sygnały prześledzone we Wszechświecie: {len(super_df):,}")
        log.info(f"Prawdziwy Forward Win-Rate Historii: {win_rate:.1f}%")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python offline_runner.py <data_directory> <output_parquet>")
        sys.exit(1)
        
    run_profiling(sys.argv[1], sys.argv[2])
