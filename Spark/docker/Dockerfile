# Spark/ docker/ Dockerfile

FROM bitnami/spark:latest

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src

# Default command to run the streaming app
ENTRYPOINT ["python", "src/stream_app.py"]
