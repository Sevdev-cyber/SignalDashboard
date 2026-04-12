# Wizjoner ↔ Bookmap Bridge

Łączy Bookmap z Wizjoner signal_server. Sygnały z bota rysują się na heatmapie, zlecenia idą przez Rithmic (Bulenox). LLM/skrypt może wysyłać zlecenia przez lokalny socket.

## Architektura

```
┌──────────────────────────────────────────────────────────────┐
│                         MAC (lokalnie)                        │
│                                                               │
│  ┌─────────────┐    localhost:9900    ┌──────────────────┐   │
│  │ LLM / Claude │ ──── TCP ────────→ │ wizjoner_bridge   │   │
│  │ trade.py     │                    │ (Bookmap addon)   │   │
│  │ terminal     │ ←── JSON response  │                   │   │
│  └─────────────┘                     │ ● heatmap lines   │   │
│                                      │ ● auto-trade      │   │
│                                      │ ● bracket SL/TP   │   │
│                                      └────────┬──────────┘   │
│                                               │              │
│                              ┌────────────────┼──────────┐   │
│                              │   Bookmap GUI  │          │   │
│                              │   (heatmap +   ▼ orders)  │   │
│                              │   Rithmic ←→ Bulenox/CME  │   │
│                              └───────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
         │ WebSocket (wss://railway)
         ▼
┌─────────────────────┐
│   VPS (Windows)      │
│   Wizjoner           │
│   signal_server.py   │
│   NT8 + TickStreamer  │
└─────────────────────┘
```

## Wymagania

- **Bookmap Global+** ($49/mies) — potrzebny do wysyłania zleceń
- **Python 3.8+**
- **Rithmic** konto (np. Bulenox)

## Instalacja

### 1. Zainstaluj Bookmap
```bash
# Pobierz z https://bookmap.com/portal/
# Mac: otwórz .dmg, przeciągnij do Applications
```

### 2. Zainstaluj zależności Python
```bash
pip3 install bookmap websocket-client
```

### 3. Dodaj addon w Bookmap
1. Otwórz Bookmap
2. Podłącz Rithmic (login/hasło z Bulenox)
3. Otwórz wykres MNQ
4. **Settings → API Addon → Add New**
5. Wskaż plik: `wizjoner_bridge.py`
6. Włącz addon na wykresie MNQ

### 4. Skonfiguruj w panelu Bookmap
| Ustawienie | Wartość | Opis |
|---|---|---|
| **WS URL** | `wss://web-production-3ff3f.up.railway.app/ws` | Relay Wizjonera |
| **Auto-Trade** | OFF | Włącz gdy chcesz auto-zlecenia z sygnałów |
| **Contracts** | 1 | Ilość kontraktów per zlecenie |
| **Min Confidence** | 70 | Minimalny % żeby auto-trade postawił zlecenie |

## Wysyłanie zleceń

### Z terminala (trade.py)
```bash
cd /Users/sacredforest/Trading\ Setup/SignalDashboard/bookmap_addon

# Kup limit
python3 trade.py buy 24500 --sl 24480 --tp 24530

# Kup limit 2 kontrakty
python3 trade.py buy 24500 --sl 24480 --tp 24530 --qty 2

# Sprzedaj market
python3 trade.py sell market --qty 1

# Sprzedaj limit
python3 trade.py sell 24550 --sl 24570 --tp 24520

# Status (aktywne zlecenia, cena, połączenie)
python3 trade.py status

# Anuluj wszystkie zlecenia
python3 trade.py cancel_all

# Zamknij pozycję (cancel all + flatten)
python3 trade.py flatten

# Przesuń SL
python3 trade.py move_sl 24485

# Przesuń TP
python3 trade.py move_tp 24540

# Ping (sprawdź czy addon działa)
python3 trade.py ping
```

### Z Pythona / LLM / Claude
```python
import socket, json

def send_order(cmd):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("127.0.0.1", 9900))
    cmd["auth"] = "sacred"
    s.sendall(json.dumps(cmd).encode())
    response = json.loads(s.recv(8192).decode())
    s.close()
    return response

# Przykłady:
send_order({"cmd": "buy", "price": 24500, "sl": 24480, "tp": 24530, "qty": 1})
send_order({"cmd": "sell", "price": 24550, "sl": 24570, "tp": 24520, "qty": 1})
send_order({"cmd": "status"})
send_order({"cmd": "cancel_all"})
send_order({"cmd": "flatten"})
send_order({"cmd": "move_sl", "sl": 24490})
```

### Raw TCP (netcat)
```bash
# Kup
echo '{"auth":"sacred","cmd":"buy","price":24500,"sl":24480,"tp":24530,"qty":1}' | nc localhost 9900

# Status
echo '{"auth":"sacred","cmd":"status"}' | nc localhost 9900

# Flatten
echo '{"auth":"sacred","cmd":"flatten"}' | nc localhost 9900
```

### Przez SSH z VPS
```bash
ssh sacredforest@mac-ip 'echo "{\"auth\":\"sacred\",\"cmd\":\"buy\",\"price\":24500,\"sl\":24480,\"tp\":24530}" | nc localhost 9900'
```

## Komendy

| Komenda | Parametry | Opis |
|---|---|---|
| `ping` | — | Sprawdź czy addon żyje |
| `status` | — | Aktywne zlecenia, cena, WS status |
| `buy` | `price`, `sl`, `tp`, `qty`, `type` | Kup (limit/market) |
| `sell` | `price`, `sl`, `tp`, `qty`, `type` | Sprzedaj (limit/market) |
| `cancel_all` | — | Anuluj wszystkie zlecenia |
| `flatten` | — | Cancel all + close pozycji |
| `move_sl` | `sl`, `id` (opcja) | Przesuń SL |
| `move_tp` | `tp`, `id` (opcja) | Przesuń TP |

### Parametry zleceń
| Parametr | Typ | Domyślnie | Opis |
|---|---|---|---|
| `price` | float | wymagany | Cena limit (lub `"market"`) |
| `sl` | float | 0 | Stop Loss — auto-bracket po fill |
| `tp` | float | 0 | Take Profit — auto-bracket po fill |
| `qty` | int | 1 | Ilość kontraktów |
| `type` | string | `"limit"` | `"limit"` lub `"market"` |
| `auth` | string | wymagany | Token autoryzacji (`"sacred"`) |

## Co widzisz na Bookmap

Po włączeniu addon'a na wykresie MNQ:

- **Złota linia** — Entry price najlepszego sygnału Wizjonera
- **Zielona przerywana** — TP target
- **Czerwona przerywana** — SL level
- **Powiadomienia** — `▲ LONG DELTA_DIV conf=85% grade=A+ tier=GOLD`

## Auto-Trade

Gdy `Auto-Trade = ON` w ustawieniach Bookmap:

1. Wizjoner generuje sygnał (conf ≥ Min Confidence, grade A+/A/B)
2. Addon stawia **limit order** na entry price
3. Po fill → automatycznie stawia **bracket**: TP (limit) + SL (stop)
4. Max 2 jednoczesne zlecenia (konfigurowalne w kodzie)

## Bezpieczeństwo

- Command socket nasłuchuje **tylko na localhost** (127.0.0.1)
- Wymaga tokenu `auth` w każdej komendzie
- Max 2 jednoczesne zlecenia (zabezpieczenie)
- Auto-Trade domyślnie wyłączony
- Bez `flatten` pozycja nie zamyka się automatycznie

## Troubleshooting

| Problem | Rozwiązanie |
|---|---|
| `connection refused` na port 9900 | Addon nie uruchomiony w Bookmap |
| `unauthorized` | Zły token auth (domyślnie: `sacred`) |
| `no instrument subscribed` | Włącz addon na wykresie MNQ w Bookmap |
| Brak linii na heatmapie | Sprawdź WS URL w ustawieniach, Wizjoner musi działać |
| Rithmic disconnect | Max 1-2 połączenia jednocześnie. Zamknij NT8 lub poproś Bulenox o drugie |

## Pliki

```
bookmap_addon/
├── wizjoner_bridge.py   # główny addon Bookmap
├── trade.py             # CLI helper do wysyłania zleceń
└── README.md            # ta instrukcja
```
