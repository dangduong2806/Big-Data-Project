# Base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy file cần thiết
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV DATA_PATH=/app/data/train_transaction.csv
ENV MODEL_PATH=/app/model/fraud_xgb_pipeline.pkl

# Copy script
COPY train_fraud_model.py .

# Default command (có thể override)
CMD ["python", "train_fraud_model.py"]
