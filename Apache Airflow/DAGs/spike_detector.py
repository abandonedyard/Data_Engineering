import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from binance.client import Client
import psycopg2
import requests
from dotenv import load_dotenv

# load environment variables from .env
load_dotenv()

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL")
POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID")

# default arguments for tasks
default_args = {
    "owner": "you",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="crypto_spike_detector",
    default_args=default_args,
    description="Detect 4%+ minute spikes in Binance futures",
    schedule_interval="@minute",
    start_date=datetime(2025, 8, 2),
    catchup=False,
) as dag:

    def fetch_futures_list(**context):
        """Get all USDT futures symbols from Binance."""
        client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
        info = client.futures_exchange_info()
        symbols = [
            s["symbol"]
            for s in info["symbols"]
            if s["quoteAsset"] == "USDT" and s["contractType"] == "PERPETUAL"
        ]
        # push to XCom for next tasks
        context["ti"].xcom_push(key="symbols", value=symbols)

    def fetch_candles(**context):
        """Fetch latest 1m candle for each symbol."""
        client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
        symbols = context["ti"].xcom_pull(key="symbols", task_ids="fetch_futures_list")
        candles = []
        for sym in symbols:
            data = client.futures_klines(symbol=sym, interval=Client.KLINE_INTERVAL_1MINUTE, limit=1)[0]
            open_p, close_p, t = float(data[1]), float(data[4]), data[0]
            candles.append({"symbol": sym, "open": open_p, "close": close_p, "time": t})
        context["ti"].xcom_push(key="candles", value=candles)

    def detect_spikes(**context):
        """Filter candles with ≥ 4% rise and store in XCom."""
        candles = context["ti"].xcom_pull(key="candles", task_ids="fetch_candles")
        spikes = []
        for c in candles:
            pct = (c["close"] - c["open"]) / c["open"] * 100
            if pct >= 4:
                spikes.append({**c, "pct": pct})
        context["ti"].xcom_push(key="spikes", value=spikes)

    def store_signals(**context):
        """Save spikes to PostgreSQL."""
        spikes = context["ti"].xcom_pull(key="spikes", task_ids="detect_spikes")
        if not spikes:
            return

        # connect via Airflow connection
        conn = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DB", "postgres"),
            user=os.getenv("POSTGRES_USER", "airflow"),
            password=os.getenv("POSTGRES_PASSWORD", ""),
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
        )
        cur = conn.cursor()
        for s in spikes:
            spike_time = datetime.fromtimestamp(s["time"] / 1000)
            cur.execute(
                """
                INSERT INTO spike_signals(symbol, spike_time, open_price, close_price, pct_change)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (s["symbol"], spike_time, s["open"], s["close"], s["pct"]),
            )
        conn.commit()
        cur.close()
        conn.close()

    def notify_trader(**context):
        """Send Slack alert if there are new spikes."""
        spikes = context["ti"].xcom_pull(key="spikes", task_ids="detect_spikes")
        if not spikes:
            return

        texts = []
        for s in spikes:
            t = datetime.fromtimestamp(s["time"] / 1000).strftime("%H:%M")
            texts.append(f"{s['symbol']} ↑{s['pct']:.2f}% at {t}")
        payload = {"text": "📈 *New Spike Signals:*\n" + "\n".join(texts)}
        # quietly ignore failures
        try:
            requests.post(SLACK_WEBHOOK, json=payload, timeout=5)
        except Exception:
            pass

    t1 = PythonOperator(
        task_id="fetch_futures_list",
        python_callable=fetch_futures_list,
    )
    t2 = PythonOperator(
        task_id="fetch_candles",
        python_callable=fetch_candles,
    )
    t3 = PythonOperator(
        task_id="detect_spikes",
        python_callable=detect_spikes,
    )
    t4 = PythonOperator(
        task_id="store_signals",
        python_callable=store_signals,
    )
    t5 = PythonOperator(
        task_id="notify_trader",
        python_callable=notify_trader,
    )

    # define task dependencies
    t1 >> t2 >> t3 >> [t4, t5]
