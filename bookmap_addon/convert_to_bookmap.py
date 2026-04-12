#!/usr/bin/env python3
"""Convert Nautilus MNQ L2 data → Bookmap .simpleformat.txt

Combines trade_tick + quote_tick for full Bookmap replay with:
- Every trade (price, size, aggressor)
- Every BBO change (bid/ask depth updates)

Usage:
    # Uses Nautilus venv:
    /Users/sacredforest/Trading\ Setup/Testing\ Nautilus/venv/bin/python convert_to_bookmap.py
    /Users/sacredforest/Trading\ Setup/Testing\ Nautilus/venv/bin/python convert_to_bookmap.py --days 14
"""
import os
import sys
import json
import argparse
from datetime import timedelta

sys.path.insert(0, "/Users/sacredforest/Trading Setup/Testing Nautilus")

CATALOG_PATH = "/Users/sacredforest/Trading Setup/Testing Nautilus/catalog"
OUTPUT_DIR = os.path.expanduser("~/Library/Application Support/Bookmap/Feeds")
PIPS = 0.25
ALIAS = "MNQ"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()

    from nautilus_trader.persistence.catalog import ParquetDataCatalog
    catalog = ParquetDataCatalog(CATALOG_PATH)

    # Get all trade ticks to find date range
    print("Loading catalog...")
    trades = catalog.trade_ticks(instrument_ids=["MNQ.CME"])
    quotes = catalog.quote_ticks(instrument_ids=["MNQ.CME"])

    print(f"Total: {len(trades):,} trades, {len(quotes):,} quotes")

    # Filter to last N days
    if trades:
        last_ts = trades[-1].ts_event
        cutoff_ns = last_ts - (args.days * 24 * 3600 * 1_000_000_000)
        trades = [t for t in trades if t.ts_event >= cutoff_ns]
        quotes = [q for q in quotes if q.ts_event >= cutoff_ns]
        print(f"After {args.days}d filter: {len(trades):,} trades, {len(quotes):,} quotes")

    if not trades:
        print("No data!")
        return

    # Output path
    output = args.output or os.path.join(
        OUTPUT_DIR, f"MNQ_{args.days}days.simpleformat.txt")
    os.makedirs(os.path.dirname(output), exist_ok=True)

    # Merge trades and quotes by timestamp, write in order
    print(f"Writing to {output}...")

    inst_info = json.dumps({
        "pips": PIPS,
        "multiplier": 2.0,
        "fullName": "MNQ Micro E-mini Nasdaq-100",
        "isFullDepth": False,
        "sizeMultiplier": 1.0,
        "isCrypto": False,
    })

    trade_count = 0
    depth_count = 0

    with open(output, 'w') as f:
        # Instrument header
        f.write(f"{trades[0].ts_event};;;onInstrumentAdded;;;{ALIAS};;;{inst_info}\n")

        # Build sorted event list (trades + quote BBO changes)
        # Trades → onTrade
        # Quotes → 2x onDepth (bid side + ask side)

        ti = 0  # trade index
        qi = 0  # quote index
        total = len(trades) + len(quotes)
        last_pct = -1

        while ti < len(trades) or qi < len(quotes):
            # Pick next event by timestamp
            t_ts = trades[ti].ts_event if ti < len(trades) else float('inf')
            q_ts = quotes[qi].ts_event if qi < len(quotes) else float('inf')

            if t_ts <= q_ts and ti < len(trades):
                # Trade event
                t = trades[ti]
                price_level = float(t.price) / PIPS
                size = int(t.size)
                is_buy = bool(t.aggressor_side)  # 1=buy, 0/2=sell

                trade_info = json.dumps({
                    "isOtc": False,
                    "isBidAggressor": is_buy,
                    "isExecutionStart": True,
                    "isExecutionEnd": True,
                })
                f.write(f"{t.ts_event};;;onTrade;;;{ALIAS};;;{price_level};;;{size};;;{trade_info}\n")
                trade_count += 1
                ti += 1
            else:
                # Quote event → 2 depth updates (bid + ask)
                q = quotes[qi]
                bid_level = int(float(q.bid_price) / PIPS)
                ask_level = int(float(q.ask_price) / PIPS)
                bid_size = int(q.bid_size)
                ask_size = int(q.ask_size)

                f.write(f"{q.ts_event};;;onDepth;;;{ALIAS};;;true;;;{bid_level};;;{bid_size}\n")
                f.write(f"{q.ts_event};;;onDepth;;;{ALIAS};;;false;;;{ask_level};;;{ask_size}\n")
                depth_count += 2
                qi += 1

            # Progress
            done = ti + qi
            pct = done * 100 // total
            if pct != last_pct and pct % 10 == 0:
                print(f"  {pct}%...", end="", flush=True)
                last_pct = pct

    file_mb = os.path.getsize(output) / (1024 * 1024)
    print(f"\n\nDone!")
    print(f"  Trades: {trade_count:,}")
    print(f"  Depth:  {depth_count:,}")
    print(f"  File:   {output} ({file_mb:.0f} MB)")
    print(f"\nTo load in Bookmap:")
    print(f"  1. Click 'OPEN DATA FILE'")
    print(f"  2. Type * in filename → Enter (show all files)")
    print(f"  3. Select the .simpleformat.txt file")
    print(f"\nNOTE: Bookmap needs DemoTextDataReplayProvider L0 module.")
    print(f"If not installed, copy the L0 jar to:")
    print(f"  ~/Library/Application Support/Bookmap/API/Layer0ApiModules/")


if __name__ == "__main__":
    main()
