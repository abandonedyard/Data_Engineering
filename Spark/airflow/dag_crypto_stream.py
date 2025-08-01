# Spark/ airflow/ dag_crypto_stream.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="crypto_4pct_stream",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    # Run the Docker-based Spark streaming application
    run_streamer = BashOperator(
        task_id="run_spark_stream",
        bash_command="docker-compose -f docker/docker-compose.yml up --build"
    )
