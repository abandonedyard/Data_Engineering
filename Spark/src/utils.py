# Spark/ src/ utils.py

import json
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, TimestampType
)

def parse_ws_message(message: str) -> dict:
    """
    Parse a raw WebSocket JSON message from Binance
    and extract symbol, price, and event timestamp.
    """
    payload = json.loads(message).get("data", {})
    return {
        "symbol": payload["s"],            # trading symbol, e.g. 'BTCUSDT'
        "price": float(payload["p"]),      # current price as float
        "event_time": datetime.fromtimestamp(payload["E"] / 1000.0)
    }

def get_stream_schema() -> StructType:
    """
    Return the schema for our streaming DataFrame,
    including fields for lag and percentage change.
    """
    return StructType([
        StructField("symbol", StringType(), nullable=False),
        StructField("price", DoubleType(), nullable=False),
        StructField("event_time", TimestampType(), nullable=False),
        StructField("prev_price", DoubleType(), nullable=True),
        StructField("pct_change", DoubleType(), nullable=True),
        StructField("fetched_at", TimestampType(), nullable=False),
    ])

def dict_to_row(parsed: dict) -> Row:
    """
    Convert a parsed dict into a Spark Row,
    initializing lag and pct_change as None.
    """
    return Row(
        symbol=parsed["symbol"],
        price=parsed["price"],
        event_time=parsed["event_time"],
        prev_price=None,
        pct_change=None,
        fetched_at=datetime.utcnow()
    )
