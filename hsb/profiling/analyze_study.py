"""
Analiza wyników Signal Predictive Power Study
==============================================
Odpowiada na pytania:
1. Które sygnały NAPRAWDĘ przewidują kierunek ceny?
2. Na ile punktów w przód i przez ile czasu?
3. Jaki jest "decay" predykcyjności (kiedy sygnał traci moc)?
4. Czy confidence score jest użyteczny?
5. Które konfluencje zwiększają predykcyjność?
"""

import sys
import pandas as pd
import numpy as np

WINDOWS = [1, 2, 5, 10, 15, 30, 60, 120]
TICK_SIZE = 0.25
POINT_VALUE = 2.0  # $2 per point MNQ


def load_study(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    print(f"Załadowano {len(df):,} sygnałów")
    print(f"Sygnały: {df['signal_name'].value_counts().to_dict()}")
    print(f"Kierunki: {df['direction'].value_counts().to_dict()}")
    print()
    return df


def print_section(title: str):
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)


def analyze_directional_accuracy(df: pd.DataFrame):
    """Czy sygnał poprawnie przewiduje kierunek ceny?"""
    print_section("1. TRAFNOŚĆ KIERUNKOWA — czy sygnał zgaduje kierunek?")

    rows = []
    for sig_name, grp in df.groupby('signal_name'):
        if len(grp) < 5:
            continue
        row = {'signal': sig_name, 'N': len(grp)}
        for w in WINDOWS:
            col = f'move_{w}m'
            valid = grp[col].dropna()
            if len(valid) < 3:
                row[f'correct_{w}m'] = np.nan
                continue
            correct = (valid > 0).mean() * 100
            row[f'correct_{w}m'] = correct
        rows.append(row)

    result = pd.DataFrame(rows)
    result.sort_values('correct_5m', ascending=False, inplace=True)

    print("\n% sygnałów gdzie cena poszła w przewidywanym kierunku:")
    print(f"{'Signal':20s} | {'N':>5s} | ", end="")
    for w in WINDOWS:
        print(f"{'@'+str(w)+'m':>7s} | ", end="")
    print()
    print("-" * 100)

    for _, r in result.iterrows():
        print(f"{r['signal']:20s} | {int(r['N']):5d} | ", end="")
        for w in WINDOWS:
            val = r.get(f'correct_{w}m', np.nan)
            if pd.isna(val):
                print(f"{'---':>7s} | ", end="")
            else:
                marker = "✓" if val > 55 else ("✗" if val < 45 else "~")
                print(f"{val:5.1f}%{marker} | ", end="")
        print()


def analyze_move_magnitude(df: pd.DataFrame):
    """Ile punktów w średniej przesuwa się cena w kierunku sygnału?"""
    print_section("2. WIELKOŚĆ RUCHU — ile punktów w średniej w kierunku sygnału?")

    rows = []
    for sig_name, grp in df.groupby('signal_name'):
        if len(grp) < 5:
            continue
        row = {'signal': sig_name, 'N': len(grp)}
        for w in WINDOWS:
            move_col = f'move_{w}m'
            mfe_col = f'mfe_{w}m'
            mae_col = f'mae_{w}m'
            valid_move = grp[move_col].dropna()
            valid_mfe = grp[mfe_col].dropna()
            valid_mae = grp[mae_col].dropna()
            if len(valid_move) < 3:
                continue
            row[f'avg_move_{w}m'] = valid_move.mean()
            row[f'med_move_{w}m'] = valid_move.median()
            row[f'avg_mfe_{w}m'] = valid_mfe.mean()
            row[f'avg_mae_{w}m'] = valid_mae.mean()
            # Edge ratio: MFE / MAE — >1 znaczy "sygnał ma przewagę"
            avg_mae = valid_mae.mean()
            row[f'edge_{w}m'] = valid_mfe.mean() / avg_mae if avg_mae > 0 else np.inf
        rows.append(row)

    result = pd.DataFrame(rows)
    result.sort_values('avg_move_5m', ascending=False, inplace=True)

    # Tabela 1: Średni ruch (punkty)
    print("\nŚredni ruch w kierunku sygnału (punkty, + = dobrze, - = źle):")
    print(f"{'Signal':20s} | {'N':>5s} | ", end="")
    for w in WINDOWS:
        print(f"{'@'+str(w)+'m':>8s} | ", end="")
    print()
    print("-" * 110)
    for _, r in result.iterrows():
        print(f"{r['signal']:20s} | {int(r['N']):5d} | ", end="")
        for w in WINDOWS:
            val = r.get(f'avg_move_{w}m', np.nan)
            if pd.isna(val):
                print(f"{'---':>8s} | ", end="")
            else:
                print(f"{val:+7.1f}p | ", end="")
        print()

    # Tabela 2: Edge ratio (MFE/MAE)
    print("\nEdge Ratio (MFE/MAE) — >1.0 = sygnał ma przewagę:")
    print(f"{'Signal':20s} | {'N':>5s} | ", end="")
    for w in WINDOWS:
        print(f"{'@'+str(w)+'m':>7s} | ", end="")
    print()
    print("-" * 100)
    for _, r in result.iterrows():
        print(f"{r['signal']:20s} | {int(r['N']):5d} | ", end="")
        for w in WINDOWS:
            val = r.get(f'edge_{w}m', np.nan)
            if pd.isna(val):
                print(f"{'---':>7s} | ", end="")
            else:
                marker = "✓" if val > 1.2 else ("✗" if val < 0.8 else "~")
                print(f"{val:5.2f}{marker} | ", end="")
        print()


def analyze_decay(df: pd.DataFrame):
    """Kiedy sygnał traci moc predykcyjną?"""
    print_section("3. DECAY PREDYKCYJNOŚCI — kiedy sygnał 'umiera'?")
    print("\nPeak MFE (średnia) per okno — kiedy sygnał osiąga max ruch korzystny:")

    for sig_name, grp in df.groupby('signal_name'):
        if len(grp) < 5:
            continue
        mfe_vals = []
        for w in WINDOWS:
            mfe_col = f'mfe_{w}m'
            val = grp[mfe_col].dropna().mean()
            mfe_vals.append((w, val))

        # Znajdź okno z max MFE
        peak_window, peak_val = max(mfe_vals, key=lambda x: x[1] if not np.isnan(x[1]) else -1)

        print(f"\n{sig_name} (N={len(grp)}):")
        for w, v in mfe_vals:
            bar_len = int(v / max(peak_val, 1) * 30) if not np.isnan(v) else 0
            bar = "█" * bar_len
            peak_mark = " ← PEAK" if w == peak_window else ""
            print(f"  @{w:3d}m: MFE={v:7.1f}pt  {bar}{peak_mark}")


def analyze_confidence_bins(df: pd.DataFrame):
    """Czy confidence score jest użyteczny — wyższy conf = lepsza predykcja?"""
    print_section("4. CONFIDENCE vs PREDYKCYJNOŚĆ — czy wyższy score = lepsza predykcja?")

    bins = [0, 50, 60, 70, 80, 90, 101]
    labels = ['<50', '50-60', '60-70', '70-80', '80-90', '90+']
    df['conf_bin'] = pd.cut(df['confidence'], bins=bins, labels=labels, right=False)

    print(f"\n{'Conf Bin':>10s} | {'N':>5s} | {'Correct@5m':>10s} | {'Correct@15m':>11s} | {'Correct@30m':>11s} | {'AvgMove@5m':>10s} | {'AvgMove@30m':>11s}")
    print("-" * 85)

    for label in labels:
        sub = df[df['conf_bin'] == label]
        if len(sub) < 3:
            continue
        n = len(sub)
        c5 = (sub['move_5m'].dropna() > 0).mean() * 100
        c15 = (sub['move_15m'].dropna() > 0).mean() * 100
        c30 = (sub['move_30m'].dropna() > 0).mean() * 100
        m5 = sub['move_5m'].dropna().mean()
        m30 = sub['move_30m'].dropna().mean()
        print(f"{label:>10s} | {n:5d} | {c5:9.1f}% | {c15:10.1f}% | {c30:10.1f}% | {m5:+9.1f}p | {m30:+10.1f}p")


def analyze_by_direction(df: pd.DataFrame):
    """Rozbicie long vs short — czy sygnały są symetryczne?"""
    print_section("5. LONG vs SHORT — asymetria predykcji")

    for sig_name, grp in df.groupby('signal_name'):
        if len(grp) < 5:
            continue
        print(f"\n{sig_name}:")
        for d in ['long', 'short']:
            sub = grp[grp['direction'] == d]
            if len(sub) < 3:
                continue
            c5 = (sub['move_5m'].dropna() > 0).mean() * 100
            c30 = (sub['move_30m'].dropna() > 0).mean() * 100
            m5 = sub['move_5m'].dropna().mean()
            m30 = sub['move_30m'].dropna().mean()
            mfe30 = sub['mfe_30m'].dropna().mean()
            mae30 = sub['mae_30m'].dropna().mean()
            print(f"  {d:5s}: N={len(sub):4d} | Correct@5m={c5:5.1f}% | @30m={c30:5.1f}% | "
                  f"Move@5m={m5:+6.1f}p | @30m={m30:+7.1f}p | MFE@30m={mfe30:6.1f}p | MAE@30m={mae30:6.1f}p")


def analyze_confluences(df: pd.DataFrame):
    """Które konfluencje zwiększają predykcyjność?"""
    print_section("6. WPŁYW KONFLUENCJI na predykcyjność")

    # Confluence count
    print("\nLiczba konfluencji vs trafność:")
    for cc, grp in df.groupby('confluence_count'):
        if len(grp) < 5:
            continue
        c5 = (grp['move_5m'].dropna() > 0).mean() * 100
        c30 = (grp['move_30m'].dropna() > 0).mean() * 100
        m30 = grp['move_30m'].dropna().mean()
        print(f"  Confluences={int(cc):2d}: N={len(grp):5d} | Correct@5m={c5:5.1f}% | @30m={c30:5.1f}% | Move@30m={m30:+7.1f}p")

    # Individual confluences
    print("\nPojedyncze konfluencje — efekt na trafność @30m:")
    from collections import defaultdict
    conf_correct = defaultdict(lambda: {'correct': 0, 'total': 0, 'moves': []})

    for _, row in df.iterrows():
        confs = row['confluences'].split('|') if row.get('confluences') else []
        move30 = row.get('move_30m', np.nan)
        if pd.isna(move30):
            continue
        for c in confs:
            if not c:
                continue
            conf_correct[c]['total'] += 1
            if move30 > 0:
                conf_correct[c]['correct'] += 1
            conf_correct[c]['moves'].append(move30)

    rows = []
    for c, stats in conf_correct.items():
        if stats['total'] < 10:
            continue
        rows.append({
            'confluence': c,
            'N': stats['total'],
            'correct_pct': stats['correct'] / stats['total'] * 100,
            'avg_move': np.mean(stats['moves']),
        })

    if rows:
        cdf = pd.DataFrame(rows).sort_values('avg_move', ascending=False)
        print(f"  {'Confluence':25s} | {'N':>5s} | {'Correct@30m':>11s} | {'AvgMove@30m':>11s}")
        print("  " + "-" * 65)
        for _, r in cdf.iterrows():
            marker = "✓" if r['correct_pct'] > 55 else ("✗" if r['correct_pct'] < 45 else "~")
            print(f"  {r['confluence']:25s} | {int(r['N']):5d} | {r['correct_pct']:9.1f}%{marker} | {r['avg_move']:+10.1f}p")


def analyze_regime(df: pd.DataFrame):
    """Jak reżim rynkowy wpływa na predykcyjność?"""
    print_section("7. REŻIM RYNKOWY — kiedy sygnały działają lepiej/gorzej?")

    for regime, grp in df.groupby('regime'):
        if len(grp) < 5:
            continue
        c5 = (grp['move_5m'].dropna() > 0).mean() * 100
        c30 = (grp['move_30m'].dropna() > 0).mean() * 100
        m5 = grp['move_5m'].dropna().mean()
        m30 = grp['move_30m'].dropna().mean()
        print(f"  {regime:15s}: N={len(grp):5d} | Correct@5m={c5:5.1f}% | @30m={c30:5.1f}% | Move@5m={m5:+6.1f}p | @30m={m30:+7.1f}p")


def summary_verdict(df: pd.DataFrame):
    """Podsumowanie: które sygnały warto trzymać."""
    print_section("WERDYKT — które sygnały naprawdę przewidują ruch ceny?")

    print("\nKryteria: Correct@5m > 55% AND avg_move@30m > 0 AND Edge@30m > 1.0\n")

    for sig_name, grp in df.groupby('signal_name'):
        if len(grp) < 5:
            continue
        c5 = (grp['move_5m'].dropna() > 0).mean() * 100
        c30 = (grp['move_30m'].dropna() > 0).mean() * 100
        m5 = grp['move_5m'].dropna().mean()
        m30 = grp['move_30m'].dropna().mean()
        mfe30 = grp['mfe_30m'].dropna().mean()
        mae30 = grp['mae_30m'].dropna().mean()
        edge = mfe30 / mae30 if mae30 > 0 else 0

        passed = c5 > 55 and m30 > 0 and edge > 1.0
        status = "PASS ✓" if passed else "FAIL ✗"

        print(f"  {sig_name:20s}: {status} | N={len(grp):5d} | Correct@5m={c5:5.1f}% | "
              f"Move@30m={m30:+7.1f}p | Edge@30m={edge:.2f}")

        if passed:
            # Dodatkowo: per direction
            for d in ['long', 'short']:
                sub = grp[grp['direction'] == d]
                if len(sub) < 3:
                    continue
                sc5 = (sub['move_5m'].dropna() > 0).mean() * 100
                sm30 = sub['move_30m'].dropna().mean()
                print(f"    └─ {d:5s}: N={len(sub):4d} | Correct@5m={sc5:5.1f}% | Move@30m={sm30:+7.1f}p")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "study_all.parquet"
    df = load_study(path)

    analyze_directional_accuracy(df)
    analyze_move_magnitude(df)
    analyze_decay(df)
    analyze_confidence_bins(df)
    analyze_by_direction(df)
    analyze_confluences(df)
    analyze_regime(df)
    summary_verdict(df)
