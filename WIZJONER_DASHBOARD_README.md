# 🔮 WIZJONER & DASHBOARD — STANDALONE ARCHITECTURE

Ten folder zawiera **kompletny, samowystarczalny system analityczny i wizualizacyjny** dla bota giełdowego (MNQ/NQ). 
Odcięliśmy wszystkie fizyczne zależności od zewnętrznych katalogów (takich jak `HybridSuperBotV2`), tworząc przenośną jednostkę bojową, którą możesz wrzucić na dowolny VPS (Linux/Windows) lub do Railway.

---

## 📂 STRUKTURA KATALOGÓW I PLIKÓW

Całość dzieli się na dwie główne warstwy: **Oczy (Dashboard)** oraz **Mózg (Wizjoner - silnik hsb)**.

### 🧠 Mózg (Wizjoner / Signal Engine)
Te pliki są odpowiedzialne za czystą matematykę, analizę ticków i wykrywanie setupów (PULLBACK, EXHAUSTION, DELTA ACCELERATION).

*   `signal_engine.py` — Główny menedżer Wizjonera. Pobiera surowe świece, zarządza historią pamięci sygnałów, ubija szkodliwe sygnały (trend filters) i normalizuje "Confidence" (0-100%).
*   `hsb/` (Katalog) — Jądro analityczne skopiowane z bota giełdowego:
    *   `hsb/signals/` — Matematyczne generatory sygnałów. (Tu znajdziesz `composite.py` grupujący logikę konfluencji oraz algorytmy m.in. RSI, VWAP).
    *   `hsb/pipeline/` — Moduły budujące rynkowy `Context` (np. profilowanie reżimu: trend vs chop).
    *   `hsb/profiling/offline_runner.py` — Symulator (Profiler), którego używamy do zrzucania i testowania na sucho milionów ticków z plików `.csv.gz` do formy `parquet`.
*   `bar_builder.py` — Fabryka dopełniająca OHLCV wskaźnikami: oblicza w locie `cum_delta`, RSI(14), EMA, VWAP na podstawie uderzeń wolumenu.
*   `session_levels.py` / `structure_filter.py` — Moduły lokalizujące sesyjne ekstrema z azjatyckich i europejskich sesji.

### 👁️ Oczy (Web Dashboard)
Te pliki przyjmują obrobione przez Wizjonera sygnały i emitują je po sieci do pięknego interfejsu przeglądarki.

*   `signal_server.py` — Serce przepływu sieciowego. Posiada dwa tryby:
    *   **Tryb LIVE**: (Używa `tcp_adapter.py`) Łączy się z NinjaTraderem przez TCP (`TickStreamerMirror`), podbiera tick po ticku rynek rzeczywisty i wysyła pakiety do podpiętych klientów front-endu przez WebSocket oraz REST Relay.
    *   **Tryb DEMO**: Symuluje rynek algorytmem błądzenia losowego, co generuje sztuczne świece i sztuczne sygnały PULLBACK (przydatne do debugowania UI).
*   `index_railway.html` — Główny front-end dashboardu (Ciemny motyw, Lightweight Charts, lista sygnałów i wolumenu w oparciu o React/Vanilla JS). Odbiera surowy JSON i maluje go na ekranie.
*   `railway_relay.py` (i `railway.json`) — Pliki deploymentowe dla chmury Railway, służą jako serwer pomostowy utrzymujący WebSocket żywym dla interfejsu.

---

## ⚙️ JAK TO DZIAŁA W PRAKTYCE? (Cykl Życia Sygnału)

1.  **NARODZINY (NinjaTrader / TCP)**: Gdy na giełdzie dochodzi do transakcji, router `TickStreamerMirror` ze włączonego softu NinjaTrader wysyła linijkę tekstu przez port TCP.
2.  **INKUBACJA (`tcp_adapter.py` -> `bar_builder.py`)**: Serwer w Pythonie przechwytuje ten ruch. Buduje świecę 5-minutową (M5) obłożoną w kalkulacje Cumulative Delta.
3.  **OCENA (`Wizjoner` / `hsb`)**: Każde zamknięcie świecy budzi Wizjonera. Oblicza on nachylenia rzeki (EMA50), reżim rynku i puszcza weryfikatory PULLBACK, FVG, SWEEP. Jeżeli odrzuca – sygnał ginie. 
4.  **ROZGŁOSZENIE (`signal_server.py`)**: Wizjoner kompresuje listę topowych wyników + 800 świec zysku w JSON. `signal_server` wysyła to do chmury / lokalnego przeglądarkowego websocketu.
5.  **EGZEKUCJA WIZUALNA (`index_railway.html`)**: Twój monitor odświeża dzwonkiem wskaźnik i zaznacza punkt wejścia na wykresie.
