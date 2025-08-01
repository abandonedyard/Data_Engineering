# Spark/ src/ stream_app.py

import threading
import json
from websocket import create_connection
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, expr, current_timestamp
from pyspark.sql.window import Window

from config import BINANCE_WS_URL, SYMBOLS, THRESHOLD_PCT, LAG_WINDOW_MS
from utils import parse_ws_message, get_stream_schema, dict_to_row

def run_ws_stream(spark):
    """
    Open a WebSocket to Binance, subscribe to tickers,
    and continuously insert incoming rows into an in-memory table.
    """
    ws = create_connection(BINANCE_WS_URL)
    # Subscribe to ticker stream for each symbol
    params = {
        "method": "SUBSCRIBE",
        "params": [f"{s.lower()}@ticker" for s in SYMBOLS],
        "id": 1
    }
    ws.send(json.dumps(params))

    while True:
        msg = ws.recv()
        parsed = parse_ws_message(msg)
        row = dict_to_row(parsed)
        # Create a tiny DataFrame and merge into our main stream table
        spark.createDataFrame([row], schema=get_stream_schema()) \
             .createOrReplaceTempView("tmp_stream")
        spark.sql("INSERT INTO memory.stream_view SELECT * FROM tmp_stream")

def main():
    # Initialize SparkSession with the app name
    spark = SparkSession.builder \
        .appName("Crypto4PctDetector") \
        .getOrCreate()

    # Prepare an empty in-memory table for streaming inserts
    spark.readStream \
         .schema(get_stream_schema()) \
         .format("memory") \
         .load() \
         .createOrReplaceTempView("stream_view")

    # Start the WebSocket listener in a background thread
    threading.Thread(target=run_ws_stream, args=(spark,), daemon=True).start()

    # Define a window spec for lag calculation per symbol
    window_spec = Window.partitionBy("symbol").orderBy(col("event_time"))

    # Compute previous price and percentage change
    df = spark.table("stream_view") \
        .withColumn("prev_price", lag("price", 1).over(window_spec)) \
        .withColumn(
            "pct_change",
            expr("(price - prev_price) / prev_price * 100")
        ) \
        .withColumn("fetched_at", current_timestamp())

    # Filter signals where pct_change >= threshold
    signals = df.filter(col("pct_change") >= THRESHOLD_PCT) \
                .select("symbol", "price", "prev_price", "pct_change", "event_time", "fetched_at")

    # Output signals to console (replace with your sink as needed)
    query = signals.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
