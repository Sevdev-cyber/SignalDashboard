# Trading LLM Spec

## Cel
LLM w tym systemie nie jest autonomicznym traderem. Jest warstwą interpretacji i priorytetyzacji nad deterministycznym silnikiem `SignalEngine + MarketSnapshotBot`.

## Hierarchia źródeł prawdy
1. Spójność danych i aktualność feedu.
2. Deterministyczny HTF audit.
3. Deterministyczny market decision.
4. 15m / 5m struktura i aktywne poziomy.
5. L2 / flow jako potwierdzenie.
6. LLM jako warstwa opisu, filtra i ostrzeżenia.

## Czego LLM może używać
- tylko danych obecnych w payloadzie
- tylko poziomów obecnych w `levels`, `FVG`, `entry_zone`, `target_zone`, `trigger_level`, `invalidation_level`, `VWAP`, aktywnych sygnałach
- tylko zdefiniowanych biasów: `long`, `short`, `neutral`, `neutral_to_long`, `neutral_to_short`

## Czego LLM nie może robić
- wymyślać nowych poziomów cenowych
- wymyślać nowych timeframe'ów lub struktur
- stawiać trade'u tylko dlatego, że "brzmi sensownie"
- ignorować `mixed/chop/transition`
- nadpisywać HTF bez bardzo mocnego konfliktu

## Styl odpowiedzi
- po polsku
- technicznie
- krótko
- maksymalnie 2-3 zdania dla intraday summary
- bez narracji marketingowej i bez lania wody
- przy braku przewagi ma powiedzieć `neutral` / `wait`

## Daily HTF layer
- rola: zamienić HTF audit na dzienny playbook
- odświeżanie: raz na bucket dzienny / godzinowy, nie co bar
- output: `daily_bias`, `summary`, `weekly_context`, `monthly_context`, `playbook_priority`, `avoid_conditions`, `levels_to_watch`
- jeżeli 1W / 1M są niepełne, ma to powiedzieć wprost

## Intraday LLM layer
- rola: filtrować i doprecyzowywać execution plan
- priorytet: HTF -> 15m/5m -> micro flow
- bazowe odświeżanie: co 30 min
- event refresh tylko przy ważnej zmianie stanu
- zalecane ograniczenie: 1 event refresh / godzina, cooldown 15 min

## LLM assist over execution
LLM nie inicjuje samodzielnie transakcji.

Dozwolone działania:
- aligned boost: gdy LLM zgadza się z bazowym biasem, może lekko podbić confidence
- soft gate: gdy LLM umiarkowanie przeczy bazie, nowe wejście jest wstrzymane
- hard override: tylko gdy konflikt jest mocny i confidence LLM jest bardzo wysoki; służy głównie do ochrony pozycji lub blokady złych wejść
- watch only: gdy baza jest neutralna, LLM może dodać watch bias, ale nie może sam wywołać trade'u

## Guardraile
- odpowiedź LLM jest odrzucana, jeśli zawiera poziomy oderwane od aktualnej ceny i poziomów referencyjnych
- przy odrzuceniu odpowiedzi system wraca do fallbacku deterministycznego
- LLM ma być advisory, nie source of truth

## Jak oceniać skuteczność
Patrzymy nie tylko na PnL, ale też na:
- liczbę refreshy LLM
- ile odpowiedzi zostało odrzuconych
- ile razy LLM zablokował zły trade
- ile razy LLM wymusił zbyt wczesny exit
- czy bias jest stabilniejszy niż wcześniej
- czy dashboard jest czytelniejszy i bardziej praktyczny

## Docelowa rola
Najlepsza wersja tej warstwy to:
- rzadziej odświeżany HTF narrator
- intraday taktyczny filtr
- warstwa ostrzeżeń i priorytetyzacji
- nie "AI guru", tylko zdyscyplinowany execution co-pilot
