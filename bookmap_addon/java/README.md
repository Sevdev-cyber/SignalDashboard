# Wizjoner Bookmap Bridge (Java addon)

Stabilny Java addon do Bookmap — streamuje orderbook, trades, delta, imbalance do Wizjonera.

## Build

```bash
cd /Users/sacredforest/Trading\ Setup/SignalDashboard/bookmap_addon/java
gradle jar
# Output: build/libs/wizjoner-bridge-1.0.jar
```

## Wymagania
- Java 11+
- Gradle
- Bookmap zainstalowany

## Install

1. Build: `gradle jar`
2. Bookmap → Settings → Configure add-ons → **Add...** → wybierz `build/libs/wizjoner-bridge-1.0.jar`
3. Zaznacz checkbox → włącz na wykresie MNQ

## Co streamuje (TCP port 9901, JSON lines)

```json
{"type":"trade","price":24500.25,"size":3,"buy":true,"ts":1712345678}
{"type":"depth","bid":true,"price":24500.0,"size":150}
{"type":"stats","delta":150,"buyVol":3500,"sellVol":3350,"imbalance":0.022,"largeBuy":5,"largeSell":2,"bid":24500.0,"ask":24500.25,"ts":1712345679}
```

## Podłączenie Wizjonera

Wizjoner (signal_server.py) łączy się na `localhost:9901` i czyta JSON lines:
```python
import socket, json
s = socket.socket()
s.connect(("localhost", 9901))
for line in s.makefile():
    data = json.loads(line)
    if data["type"] == "stats":
        print(f"Delta={data['delta']} Imbalance={data['imbalance']:.3f}")
```
