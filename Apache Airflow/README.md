# Apache Airflow Crypto Spike Detector

This project uses Apache Airflow to automatically detect minute-long price spikes (≥ 4%) in Binance USDT futures and send alerts.

## Features

- Fetches the list of all USDT perpetual futures symbols from Binance API every minute  
- Retrieves the latest 1-minute candle (open/close) for each symbol  
- Calculates the percentage change and filters for spikes ≥ 4%  
- Stores detected spikes in PostgreSQL  
- Sends real-time notifications to Slack or Telegram  
- Generates a daily summary report of spike statistics

## Prerequisites

- Python 3.8+  
- Docker (optional, for local Airflow setup)  
- PostgreSQL database  
- Binance API key & secret  
- Slack or Telegram webhook URL

## Installation

1. Clone the repository:  
   ```bash
   git clone https://github.com/yourname/apache-airflow.git
   cd apache-airflow
2. Create and activate a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
4. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
5. Copy the environment template and fill in your credentials:
   ```bash
   cp .env.template .env
6. Initialize the PostgreSQL database:
   ```bash
   psql -U your_user -d your_db -f scripts/init_db.sql

