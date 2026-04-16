import pandas as pd
from bar_builder import enrich_bars

df = pd.DataFrame([
    {"datetime": "2024-04-10 09:30:00", "open": 100, "high": 105, "low": 95, "close": 104, "volume": 100, "delta": 0},
    {"datetime": "2024-04-10 09:35:00", "open": 104, "high": 108, "low": 100, "close": 101, "volume": 200, "delta": 0},
])
df["datetime"] = pd.to_datetime(df["datetime"])
res = enrich_bars(df)
print(res[["datetime", "close", "delta", "cum_delta"]])
