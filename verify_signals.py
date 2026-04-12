"""Quick signal verification — run on VPS to check what engine produces."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
import numpy as np
from datetime import datetime
from signal_engine import SignalEngine
from bar_builder import enrich_bars

np.random.seed(123)
n = 100
base = 24300.0
prices = [base]
for i in range(n - 1):
    prices.append(prices[-1] + np.random.randn() * 8)

df = pd.DataFrame({
    'datetime': pd.date_range('2026-04-06 09:35', periods=n, freq='5min'),
    'open': [p - np.random.rand() * 4 for p in prices],
    'high': [p + np.random.rand() * 8 + 3 for p in prices],
    'low': [p - np.random.rand() * 8 - 3 for p in prices],
    'close': prices,
    'volume': np.random.randint(500, 3000, n).tolist(),
    'delta': (np.random.randn(n) * 200).astype(int).tolist(),
})

enriched = enrich_bars(df)
engine = SignalEngine()
now = datetime(2026, 4, 6, 10, 30)
signals = engine.evaluate(enriched, current_price=prices[-1], now=now)

print(f"\n{'='*110}")
print(f"SIGNAL ENGINE OUTPUT — {len(signals)} signals from {n} bars")
print(f"Price: {prices[-1]:.2f} | ATR: {enriched['atr'].iloc[-1]:.1f} | "
      f"CVD: {enriched['cum_delta'].iloc[-1]:.0f} | VWAP: {enriched['vwap'].iloc[-1]:.2f}")
print(f"{'='*110}")

if not signals:
    print("NO SIGNALS GENERATED!")
else:
    print(f"{'#':>2} {'Signal':<22} {'Dir':>5} {'Conf':>4}% {'Entry':>10} "
          f"{'SL':>10} {'TP1':>10} {'Risk':>5} {'RR':>4} {'Grade':>5} "
          f"{'Tier':>6} {'Speed':>8} {'Hold':>5}")
    print("-" * 110)
    for i, s in enumerate(signals[:20]):
        rr = abs(s['tp1'] - s['entry']) / s['risk_pts'] if s['risk_pts'] > 0 else 0
        print(f"{i+1:>2} {s['name']:<22} {s['direction']:>5} {s['confidence_pct']:>4}% "
              f"{s['entry']:>10.2f} {s['sl']:>10.2f} {s['tp1']:>10.2f} "
              f"{s['risk_pts']:>5.1f} {rr:>4.1f} {s.get('quality_grade','?'):>5} "
              f"{s.get('tier_label',''):>6} {s.get('speed_label','?'):>8} "
              f"{s.get('optimal_min','?'):>5}")

    # Summary by type
    print(f"\n{'='*60}")
    print("SUMMARY BY SIGNAL TYPE:")
    print(f"{'='*60}")
    from collections import Counter
    by_type = Counter()
    by_dir = Counter()
    for s in signals:
        by_type[s['name']] += 1
        by_dir[s['direction']] += 1

    for name, cnt in by_type.most_common():
        sigs = [s for s in signals if s['name'] == name]
        avg_conf = sum(s['confidence_pct'] for s in sigs) / len(sigs)
        grades = [s.get('quality_grade', '?') for s in sigs]
        print(f"  {name:<22} x{cnt:>2} | avg_conf={avg_conf:.0f}% | grades={','.join(grades[:5])}")

    print(f"\n  Direction: {dict(by_dir)}")
    gold = sum(1 for s in signals if s.get('tier_label') == 'GOLD')
    silver = sum(1 for s in signals if s.get('tier_label') == 'SILVER')
    print(f"  Tiers: GOLD={gold}, SILVER={silver}, none={len(signals)-gold-silver}")

    # Check SL/TP distances
    print(f"\n{'='*60}")
    print("SL/TP DISTANCE ANALYSIS:")
    print(f"{'='*60}")
    for s in signals[:10]:
        sl_dist = abs(s['entry'] - s['sl'])
        tp_dist = abs(s['tp1'] - s['entry'])
        atr = s.get('atr', 20)
        print(f"  {s['name']:<22} SL={sl_dist:.1f}pts ({sl_dist/atr:.2f}xATR) "
              f"TP={tp_dist:.1f}pts ({tp_dist/atr:.2f}xATR) RR={tp_dist/sl_dist:.1f}:1")
