# Signal Dashboard — Deploy & Architecture Guide

## Architektura

```
┌─────────────────────────────────────────────────────────┐
│                    VPS (Windows)                         │
│                 66.42.117.137                            │
│                                                         │
│  ┌──────────────────┐    TCP:5557    ┌───────────────┐  │
│  │ NT8               │ ────────────→ │ Wizjoner       │  │
│  │ TickStreamerMirror │ ← ticki/bary │ signal_server  │  │
│  │ (chart 1min)      │              │ (Python)       │  │
│  └──────────────────┘              │                 │  │
│                                     │  WS:8082       │  │
│                                     │  ↓             │  │
│                                     │  HTTP POST ──────────→ Railway Relay
│                                     └───────────────┘  │
└─────────────────────────────────────────────────────────┘
           │                                    │
           │ WS:8082 (local)                    │ HTTPS POST
           ▼                                    ▼
   ┌──────────────┐              ┌──────────────────────┐
   │ Local browser │              │ Railway               │
   │ (VPS RDP)     │              │ web-production-3ff3f   │
   └──────────────┘              │                        │
                                  │ railway_relay.py       │
                                  │ index_railway.html     │
                                  │         ↓ WS           │
                                  │ ┌──────────────────┐   │
                                  │ │ Browser (Mac/tel) │   │
                                  │ └──────────────────┘   │
                                  └──────────────────────┘
```

---

## Lokalizacje plików

### Mac (lokalny dev)
```
/Users/sacredforest/Trading Setup/
├── SignalDashboard/                    ← GŁÓWNY PROJEKT (git repo)
│   ├── signal_server.py               ← Wizjoner backend (TCP→WS→Relay)
│   ├── signal_engine.py               ← Dashboard wrapper (final_mtf / v2 / v3)
│   ├── bar_builder.py                 ← OHLCV → ATR/EMA/VWAP/Delta/CVD
│   ├── tcp_adapter.py                 ← TCP klient do NT8 TickStreamer
│   ├── compat.py                      ← Python 3.9 dataclass patch
│   ├── railway_relay.py               ← Railway relay server (deploy na Railway)
│   ├── index_railway.html             ← Railway frontend (current live dashboard)
│   ├── DEPLOY_GUIDE.md                ← Ten plik
│   ├── TRADER_GUIDE_INTEGRATION_2026-04-13.txt ← Instrukcja guide + L2
│   ├── session_levels.py              ← IB/session level calculation
│   ├── structure_filter.py            ← Swing high/low detection
│   │
│   ├── hsb/                           ← HybridSuperBot signal library
│   │   ├── signals/
│   │   │   ├── composite.py           ← Główny generator (łączy wszystkie)
│   │   │   ├── _helpers.py            ← make_signal() + SL padding
│   │   │   ├── delta_divergence.py    ← Delta divergence detector
│   │   │   ├── delta_acceleration.py  ← Delta acceleration detector
│   │   │   ├── delta_streak.py        ← Delta streak reversal (NOWY)
│   │   │   ├── exhaustion.py          ← Exhaustion reversal
│   │   │   ├── trend_continuation.py  ← Trend continuation
│   │   │   ├── vwap_bounce.py         ← VWAP bounce
│   │   │   ├── ema_bounce.py          ← EMA bounce
│   │   │   ├── waterfall.py           ← Waterfall cascade
│   │   │   ├── micro_smc.py           ← BOS/CHOCH/FVG
│   │   │   └── ib_break.py            ← IB Break/Retest (NOWY)
│   │   ├── pipeline/
│   │   │   ├── context_builder.py     ← Bars → AnalysisContext
│   │   │   └── regime.py              ← Regime detection (v2: EMA+VWAP)
│   │   ├── domain/
│   │   │   ├── models.py              ← SignalCandidate dataclass
│   │   │   ├── enums.py               ← Direction, CandidateFamily
│   │   │   └── context.py             ← RegimeInfo, BarData, etc.
│   │   └── profiling/
│   │       ├── offline_runner.py       ← Tick-by-tick backtester
│   │       ├── signal_study.py         ← Forward window event study
│   │       └── lifecycle_tracker.py    ← Signal lifecycle tracking
│   │
│   ├── bookmap_addon/                 ← Bookmap integration
│   │   ├── wizjoner_bridge.py         ← Python addon (alpha, crashuje)
│   │   ├── wizjoner_simple.py         ← Minimal Python addon
│   │   ├── trade.py                   ← CLI do zleceń przez NT8 TCP
│   │   ├── replay.py                  ← Export Nautilus→Bookmap CSV
│   │   ├── convert_to_bookmap.py      ← Konwerter danych
│   │   ├── loader.py                  ← Bookmap embedded editor loader
│   │   ├── README.md                  ← Instrukcja Bookmap addon
│   │   └── java/                      ← Java addon (stabilny)
│   │       ├── src/.../WizjonerBridge.java      ← L1 addon (TCP:9901)
│   │       ├── src/.../NautilusReplayProvider.java ← L0 replay (wymaga Quant plan)
│   │       └── build/wizjoner-bridge.jar         ← Skompilowany addon
│   │
│   ├── backtest_fast.py               ← Szybki 14d backtester
│   ├── backtest_14d.py                ← Pełny backtester (Nautilus catalog)
│   └── verify_signals.py              ← Weryfikacja sygnałów na VPS
│
├── NewSignal/                         ← NOWY ENGINE v2
│   ├── final_signal_engine.py         ← FinalSignalEngine (multi-TF variants v1/v2/v3)
│   ├── newsignal_core.py             ← Core generator (78KB, wszystkie families)
│   ├── v2/
│   │   └── signal_engine_v2.py        ← Drop-in wrapper
│   ├── backtest_final_multi_tf_tick.py ← Backtester v2
│   ├── final_mtf_tick_summary.csv     ← Wyniki backtestów
│   └── final_mtf_engine_2026-04-12.txt ← Dokumentacja engine'a
│
├── Testing Nautilus/                  ← Dane historyczne + backtesty
│   ├── catalog/data/
│   │   ├── trade_tick/MNQ.CME/        ← 126 plików, 1.3GB (od 2024-04)
│   │   ├── quote_tick/MNQ.CME/        ← 117 plików, 741MB (bid/ask)
│   │   └── order_book_deltas/MNQ.CME/ ← 814 plików, 6.7GB (L2 depth)
│   ├── signal_study/
│   │   └── signal_events.csv          ← 37,803 signal events z backtestów
│   ├── ltf_trades.csv                 ← 311 LTF strategy trades
│   └── venv/                          ← Python venv z nautilus_trader
│
└── scalper_v4_ultimate/dashboard/ninjascript/
    └── TickStreamerMirror.cs           ← NT8 indicator (TCP bridge)
```

### VPS (Windows, 66.42.117.137)
```
C:\SignalDashboard\                    ← Kopia z git/scp
├── signal_server.py                   ← Wizjoner (uruchamiany z Wizjoner.bat)
├── signal_engine.py
├── bar_builder.py
├── tcp_adapter.py
├── hsb/...                            ← Cały pakiet HSB
└── server.log                         ← Logi Wizjonera

C:\NewSignal\                          ← WYMAGANE przez signal_engine.py
├── final_signal_engine.py             ← aktywny FinalSignalEngine
├── newsignal_core.py                  ← core families + guide zones
├── v2\signal_engine_v2.py
└── *.txt / *.csv                      ← audyty i wyniki testów

C:\Users\Administrator\Desktop\
├── Wizjoner.bat                       ← Uruchamia Wizjonera
├── 1_START_Scalper_Playback.bat       ← NT8 playback
└── 2_START_Scalper_Live.bat           ← NT8 live
```

### Railway (cloud)
```
GitHub repo: Sevdev-cyber/SignalDashboard
  → auto-deploy na push do main
  → serwuje: railway_relay.py + index_railway.html
  → URL: https://web-production-3ff3f.up.railway.app
```

---

## SSH / Deploy

### Połączenie VPS
```bash
ssh Administrator@66.42.117.137
# Klucz: ~/.ssh/id_ed25519 (auto)
```

### Deploy na VPS (cały projekt)
```bash
cd "/Users/sacredforest/Trading Setup"
tar czf /tmp/w.tar.gz SignalDashboard/ NewSignal/
scp /tmp/w.tar.gz Administrator@66.42.117.137:"C:/"
ssh Administrator@66.42.117.137 "cd /d C:\ && tar xzf w.tar.gz && del w.tar.gz"
rm /tmp/w.tar.gz
echo "DEPLOYED"
```

### Deploy na VPS (pojedyncze pliki)
```bash
scp SignalDashboard/signal_server.py Administrator@66.42.117.137:"C:/SignalDashboard/"
scp SignalDashboard/signal_engine.py Administrator@66.42.117.137:"C:/SignalDashboard/"
scp SignalDashboard/bar_builder.py Administrator@66.42.117.137:"C:/SignalDashboard/"
scp NewSignal/final_signal_engine.py Administrator@66.42.117.137:"C:/NewSignal/"
scp NewSignal/newsignal_core.py Administrator@66.42.117.137:"C:/NewSignal/"
```

### Deploy na Railway (git push)
```bash
cd "/Users/sacredforest/Trading Setup/SignalDashboard"
git add -A
git commit -m "opis zmian"
git push origin main
# Railway auto-deploy w ~1 min
```

### Restart Wizjonera na VPS
```bash
# Przez SSH (kill + start):
ssh Administrator@66.42.117.137 "taskkill /F /IM python.exe"
# Potem ręcznie przez RDP: kliknij Wizjoner.bat na pulpicie

# Albo jednym poleceniem (nie zawsze działa przez SSH):
ssh Administrator@66.42.117.137 "taskkill /F /IM python.exe & timeout /t 2 & start cmd /c C:\Users\Administrator\Desktop\Wizjoner.bat"
```

### Czyszczenie cache Python na VPS
```bash
ssh Administrator@66.42.117.137 "powershell -Command \"Remove-Item -Recurse -Force 'C:\SignalDashboard\__pycache__','C:\SignalDashboard\hsb\signals\__pycache__','C:\SignalDashboard\hsb\__pycache__','C:\SignalDashboard\hsb\domain\__pycache__','C:\SignalDashboard\hsb\pipeline\__pycache__' -ErrorAction SilentlyContinue\""
```

### Sprawdzanie logów
```bash
# Ostatnie 30 linii:
ssh Administrator@66.42.117.137 "powershell -Command \"Get-Content C:\SignalDashboard\server.log -Tail 30\""

# Filtruj po BAR/Signal/Error:
ssh Administrator@66.42.117.137 "powershell -Command \"Get-Content C:\SignalDashboard\server.log | Select-String 'BAR|Signals:|error' | Select-Object -Last 20\""
```

---

## Data flow — jak płyną dane

```
NT8 (VPS)
  │
  │ TCP:5557 — bary (B;timestamp;O;H;L;C;V) + ticki (T;ts;price;vol;aggressor;bid;ask)
  │
  ▼
tcp_adapter.py — parsuje TCP stream
  │
  ├── warmup_bars → bar_builder.warmup_bars_to_df() → enrich_bars()
  │                  → apply_tick_deltas() (datetime64[ns] fix!)
  │
  ├── live bar_close → BarAccumulator (resample do aktywnego TF, rekomendowane 1min)
  │                    → enrich_bars() (ATR, EMA, VWAP, Delta, CVD)
  │
  └── live ticks → on_tick() → buy/sell vol accumulator
                               → per-signal price_min/max tracking
                               → entry_touched detection
  │
  ▼
signal_engine.py — evaluate(bars_df)
  │
  ├── domyślnie: FinalSignalEngine z katalogu NewSignal
  ├── obsługiwane tryby: final_mtf, final_mtf_v2, final_mtf_v3
  ├── rekomendowany tryb: final_mtf_v3
  ├── trader_guide: 5m / 15m bias, continuation, invalidation, best zones
  ├── L2 reaction: lekkie microstructure z live tick + bid/ask
  └── fallback: stary enrich/ranking path gdy engine_mode nie jest final_mtf*
  │
  ▼
signal_server.py — _evaluate_and_broadcast()
  │
  ├── Persistent signals (carry forward, _is_dead: SL/TP/expired)
  ├── price_min/price_max tracking (tick-level SL/TP detection)
  │
  ├── Local WS:8082 → full payload (signals + bars + state)
  └── HTTP POST → Railway relay → browser dashboard
```

---

## Wizjoner.bat (VPS startup)
```bat
@echo off
TITLE Wizjoner Signal Dashboard - Backend
cd C:\SignalDashboard
set DASHBOARD_BAR_TF_MIN=1
set SIGNAL_ENGINE_MODE=final_mtf_v3
python signal_server.py --port 5557 --ws-port 8082 --relay-url https://web-production-3ff3f.up.railway.app/push --relay-secret SacredForestSignal123
pause
```

## Ostatnia weryfikacja

- Feed wejściowy do Wizjonera: `1m`, `On each tick`, z live tickami bid/ask z TickStreamerMirror.
- Flow stack poprawiony: `delta`, `buy_volume`, `sell_volume`, `cum_delta/cvd`, `vwap` liczone z prawdziwych ticków gdy są dostępne; fallback z OHLC używany tylko gdy ticków brak.
- Session reset poprawiony do granicy CME `18:00 ET`.
- Entry execution poprawione o mikro-potwierdzenie zamiast natychmiastowego ślepego wejścia dla wybranych family.
- Smoke test micro-exec:
  - baseline 8 sesji: `825` setupów, `66.3%` TP hit, median `MAE 14.75`
  - true micro executor 8 sesji: `18` wykonań, `83.3%` TP hit, `77.8%` clean TP, median `MAE 8.38`
- Pliki z audytem:
  - `NewSignal/entry_execution_audit_2026-04-13.txt`
  - `NewSignal/flow_calculation_audit_2026-04-13.txt`

---

## NT8 TickStreamerMirror — konfiguracja

Plik: `scalper_v4_ultimate/dashboard/ninjascript/TickStreamerMirror.cs`

```
Calculate       = Calculate.OnEachTick
TcpPort         = 5557          ← musi matchować --port w Wizjoner.bat
BarsToSend      = 50            ← warmup bars
TickWarmupBars  = 80            ← ticki z ostatnich 80 barów
UseTickWarmup   = true          ← włączone (potrzebne dla delta/CVD)
AcceptCommands  = false         ← true jeśli chcesz trade.py zlecenia
AccountName     = "Sim101"      ← zmień na konto Bulenox
```

Rekomendacja:
- feed barów ustaw na `1m`
- ticki muszą zawierać `bid` i `ask`, bo trader guide używa lekkiego `L2 reaction`

Protokół TCP:
```
NT8 → Bot:   B;timestamp;O;H;L;C;V;isClosed     (warmup bar)
             BC;timestamp;O;H;L;C;V;1            (live bar close)
             T;timestamp;price;vol;aggr;bid;ask   (live tick)

Bot → NT8:   BUY_LIMIT;qty;price;signal;oco      (zlecenie)
             SELL_STOP;qty;price;signal;oco
             CLOSE                                 (flatten)
             CANCEL;signalName

NT8 → Bot:   ACK;cmd;status;details               (potwierdzenie)
```

---

## Railway — konfiguracja

**Git repo:** `https://github.com/Sevdev-cyber/SignalDashboard.git`
**URL:** `https://web-production-3ff3f.up.railway.app`
**Push secret:** `SacredForestSignal123`

Pliki na Railway:
- `railway_relay.py` — relay server (aiohttp)
- `index_railway.html` — dashboard frontend

Deploy: `git push origin main` → Railway auto-builds

Stan na 2026-04-13:
- lokalny kod ma już `Trader Guide` + `L2 Reaction`
- live Railway URL jeszcze nie pokazuje tych sekcji, więc Railway jest opóźniony względem lokalnego repo i wymaga nowego deploya

Endpointy:
- `POST /push` — Wizjoner wysyła dane (header: X-Push-Secret)
- `GET /ws` — WebSocket dla przeglądarki

---

## Kluczowe porty

| Port | Gdzie | Co |
|------|-------|----|
| 5557 | VPS localhost | NT8 → Wizjoner (TCP) |
| 8082 | VPS 0.0.0.0 | Wizjoner → browser (WebSocket, lokalnie na VPS) |
| 9901 | Mac localhost | Bookmap addon → Wizjoner (TCP) |
| 443 | Railway | Dashboard HTTPS/WSS |

---

## Python environments

| Gdzie | Python | Venv |
|-------|--------|------|
| VPS | `python.exe` | brak (systemowy) |
| Mac (backtest) | `/Users/sacredforest/Trading Setup/Testing Nautilus/venv/bin/python` | nautilus_trader |
| Mac (system) | `/usr/bin/python3` lub brew | bookmap, websocket-client |

---

## Przydatne komendy

```bash
# Status Wizjonera
ssh Administrator@66.42.117.137 "tasklist /FI \"IMAGENAME eq python.exe\""

# Szybki deploy + restart
cd "/Users/sacredforest/Trading Setup" && \
  tar czf /tmp/w.tar.gz SignalDashboard/ && \
  scp /tmp/w.tar.gz Administrator@66.42.117.137:"C:/" && \
  ssh Administrator@66.42.117.137 "cd /d C:\ && tar xzf w.tar.gz && del w.tar.gz" && \
  rm /tmp/w.tar.gz && echo "DEPLOYED"

# Test sygnałów na VPS
ssh Administrator@66.42.117.137 "cd /d C:\SignalDashboard && python verify_signals.py"

# Test z nowym engine (na VPS lub lokalnie)
DASHBOARD_BAR_TF_MIN=1 SIGNAL_ENGINE_MODE=final_mtf_v3 python signal_server.py --port 5557 --ws-port 8082

# Backtest 14 dni (na Macu)
/Users/sacredforest/Trading\ Setup/Testing\ Nautilus/venv/bin/python \
  /Users/sacredforest/Trading\ Setup/SignalDashboard/backtest_fast.py

# Export danych dla Bookmap
/Users/sacredforest/Trading\ Setup/Testing\ Nautilus/venv/bin/python \
  /Users/sacredforest/Trading\ Setup/SignalDashboard/bookmap_addon/replay.py --days 5
```
