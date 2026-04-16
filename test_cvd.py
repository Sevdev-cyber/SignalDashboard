import pandas as pd
from bar_builder import enrich_bars

df = pd.DataFrame([
    {"datetime": "2024-04-10 09:30:00", "open": 100, "high": 105, "low": 95, "close": 102, "volume": 10, "delta": 5},
    {"datetime": "2024-04-10 09:35:00", "open": 102, "high": 108, "low": 100, "close": 101, "volume": 20, "delta": -10},
])
df["datetime"] = pd.to_datetime(df["datetime"])
res = enrich_bars(df)
print(res[["datetime", "close", "delta", "cum_delta"]])
