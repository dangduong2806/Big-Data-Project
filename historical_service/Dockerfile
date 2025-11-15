FROM python:3.10-slim

# Cài đặt Java (cần cho Pyspark)
RUN apt-get update && apt-get install -y default-jre-headless && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY historical_api.py .

# Chạy API trên cổng 8001
CMD ["python", "historical_api.py"]