# Crypto 4% Spike Detector on Spark

An end-to-end PySpark Structured Streaming application that listens to Binance Futures prices for 400+ trading pairs, detects minute-over-minute price increases of 4% or more, and outputs real-time BUY signals. This project demonstrates scalable streaming ETL, window functions, and a push-button Docker deployment.

---

## Features

- **Real-time ingestion** from Binance Futures WebSocket for 400+ symbols  
- **Lag-based calculation** of minute-over-minute price change  
- **Threshold filter** for `pct_change >= 4.0%` signals  
- **Console output** of detected BUY signals (easily extendable to Delta Lake, PostgreSQL, or messaging)  
- **Dockerized** for one-command local startup  
- **Optional Airflow DAG** for orchestration

---

## Repository Structure

Spark/
├── README.md
├── requirements.txt
├── src/
│ ├── config.py
│ ├── utils.py
│ └── stream_app.py
├── docker/
│ ├── Dockerfile
│ └── docker-compose.yml
└── airflow/
└── dag_crypto_stream.py
