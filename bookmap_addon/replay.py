#!/usr/bin/env python3
"""Export Nautilus MNQ data → .mnq.csv for Bookmap replay.

Creates a CSV file that Bookmap's NautilusReplayProvider can load
directly via "OPEN DATA FILE".

Usage:
    # Export last 3 days (run with Nautilus venv):
    /Users/sacredforest/Trading\ Setup/Testing\ Nautilus/venv/bin/python replay.py

    # Export last 14 days:
    /Users/sacredforest/Trading\ Setup/Testing\ Nautilus/venv/bin/python replay.py --days 14

    # Export specific dates:
    /Users/sacredforest/Trading\ Setup/Testing\ Nautilus/venv/bin/python replay.py --start 2026-03-20 --end 2026-03-25

    # Then in Bookmap:
    #   1. Restart Bookmap (loads NautilusReplayProvider module)
    #   2. OPEN DATA FILE
    #   3. Type * in filename → Enter
    #   4. Select the .mnq.csv file
    #   5. Play!
"""
import os
import sys
import argparse

CATALOG = "/Users/sacredforest/Trading Setup/Testing Nautilus/catalog"
OUTPUT_DIR = os.path.expanduser("~/Library/Application Support/Bookmap/Feeds")


def main():
    parser = argparse.ArgumentParser(description="Export Nautilus MNQ → Bookmap CSV")
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()

    from nautilus_trader.persistence.catalog import ParquetDataCatalog
    catalog = ParquetDataCatalog(CATALOG)

    # Determine date range
    if args.start and args.end:
        start, end = args.start, args.end
        label = f"{start}_to_{end}"
    else:
        # Load minimal data to find last date
        all_trades = catalog.trade_ticks(instrument_ids=["MNQ.CME"])
        if not all_trades:
            print("No MNQ trade data in catalog!")
            return
        from datetime import datetime, timedelta
        last = datetime.utcfromtimestamp(all_trades[-1].ts_event / 1e9)
        first = last - timedelta(days=args.days)
        start = first.strftime("%Y-%m-%d")
        end = (last + timedelta(days=1)).strftime("%Y-%m-%d")
        label = f"{args.days}days"
        del all_trades  # free RAM

    print(f"Loading {start} → {end}...")
    trades = catalog.trade_ticks(instrument_ids=["MNQ.CME"], start=start, end=end)
    quotes = catalog.quote_ticks(instrument_ids=["MNQ.CME"], start=start, end=end)
    print(f"  Trades: {len(trades):,}  Quotes: {len(quotes):,}")

    if not trades:
        print("No data for this range!")
        return

    output = args.output or os.path.join(OUTPUT_DIR, f"MNQ_{label}.mnq.csv")
    os.makedirs(os.path.dirname(output), exist_ok=True)

    print(f"Writing {output}...")

    with open(output, 'w') as f:
        f.write("ts_ns,price,size,aggressor,bid,ask,bid_size,ask_size\n")

        ti, qi = 0, 0
        count = 0
        last_bid = last_ask = 0.0
        last_bsz = last_asz = 0

        while ti < len(trades) or qi < len(quotes):
            t_ts = trades[ti].ts_event if ti < len(trades) else float('inf')
            q_ts = quotes[qi].ts_event if qi < len(quotes) else float('inf')

            # Update BBO from quotes
            if q_ts <= t_ts and qi < len(quotes):
                q = quotes[qi]
                last_bid = float(q.bid_price)
                last_ask = float(q.ask_price)
                last_bsz = int(q.bid_size)
                last_asz = int(q.ask_size)
                qi += 1
                continue  # quote-only updates don't produce CSV rows with trades

            if ti < len(trades):
                t = trades[ti]
                # Check if there's a matching quote at same timestamp
                while qi < len(quotes) and quotes[qi].ts_event <= t.ts_event:
                    q = quotes[qi]
                    last_bid = float(q.bid_price)
                    last_ask = float(q.ask_price)
                    last_bsz = int(q.bid_size)
                    last_asz = int(q.ask_size)
                    qi += 1

                f.write(f"{t.ts_event},{float(t.price)},{int(t.size)},"
                        f"{int(t.aggressor_side)},"
                        f"{last_bid},{last_ask},{last_bsz},{last_asz}\n")
                count += 1
                ti += 1

            if count % 100000 == 0 and count > 0:
                print(f"  {count:,}...", end="", flush=True)

    mb = os.path.getsize(output) / (1024 * 1024)
    print(f"\n\nGotowe!")
    print(f"  {count:,} ticks → {output} ({mb:.0f} MB)")
    print(f"\nW Bookmap:")
    print(f"  1. Zrestartuj Bookmap")
    print(f"  2. OPEN DATA FILE")
    print(f"  3. Wpisz * w filename → Enter")
    print(f"  4. Wybierz: {os.path.basename(output)}")
    print(f"  5. Play!")


if __name__ == "__main__":
    main()
