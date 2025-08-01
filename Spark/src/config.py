# Spark/ src/ config.py

# Binance Futures WebSocket endpoint for streaming ticker updates
BINANCE_WS_URL = "wss://fstream.binance.com/stream"

# Threshold percentage for signaling a 4% (or greater) price move
THRESHOLD_PCT = 4.0

# Time window for lag calculation (1 minute in milliseconds)
LAG_WINDOW_MS = 60_000

# List of symbols to monitor (replace … with actual tickers or load from a file)
SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    # … add all 400+ futures symbols here …
]
