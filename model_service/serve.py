import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List
import numpy as np
from threading import Thread

import pandas as pd
import joblib
from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaConsumer, KafkaProducer

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cấu hình từ environment variables

MODEL_PATH = os.environ.get("MODEL_PATH", "./risk_model.joblib")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
INPUT_TOPIC = os.environ.get("INPUT_TOPIC", "daily_prices")  # Đọc dữ liệu real-time từ Yahoo Finance
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "predictions")
WINDOW_SIZE = 21  # window size giống như trong train_job.py

app = FastAPI(title="Real-time Risk Event Model API")

class Features(BaseModel):
    Daily_Return: float
    Volatility_Cluster: float
    Volume_Based_Volatility: float

class PriceData:
    def __init__(self):
        self.data = []
        self.last_processed = None
    
    def add_record(self, record: Dict):
        self.data.append(record)
        # Giữ window_size + 1 records để tính returns
        if len(self.data) > WINDOW_SIZE + 1:
            self.data.pop(0)
    
    def calculate_features(self) -> Features:
        if len(self.data) < WINDOW_SIZE:
            return None
        
        df = pd.DataFrame(self.data)
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values('Date')
        
        # Tính Daily Return và các features như trong train_job.py
        df['Daily_Return'] = df['Close'].pct_change()
        
        # Chỉ tính features nếu có đủ dữ liệu
        if len(df) < WINDOW_SIZE:
            return None
            
        latest = df.iloc[-1]
        window = df.iloc[-WINDOW_SIZE:]
        
        # Tính các features giống như trong train_job.py
        daily_return = latest['Daily_Return']
        volatility_cluster = window['Daily_Return'].std() * (252**0.5)  # annualized
        volume_based_volatility = window['Volume'].std() / window['Volume'].mean()
        
        return Features(
            Daily_Return=float(daily_return),
            Volatility_Cluster=float(volatility_cluster),
            Volume_Based_Volatility=float(volume_based_volatility)
        )

# Global variables
model = None
price_data = PriceData()
kafka_consumer = None
kafka_producer = None

def process_message(msg):
    try:
        if not msg.value:
            logger.warning("Skipping empty message")
            return
        
        msg_str = msg.value.decode('utf-8').strip()
        if not msg_str:
            logger.warning("Skipping empty message")
            return
        
        # record = json.loads(msg.value.decode('utf-8'))
        # Dùng msg_str đã strip
        try:
            record = json.loads(msg_str)
        except json.JSONDecodeError:
            logger.warning("Skipping invalid JSON message: %s", msg_str)
            return
        
        price_data.add_record(record)
        
        # Tính features
        features = price_data.calculate_features()
        if features is None:
            return
        
        # Dự đoán
        if model is None:
            logger.error("Model not loaded")
            return
            
        df = pd.DataFrame([features.dict()])
        proba = model.predict_proba(df)[:, 1][0]
        pred = int(proba > 0.5)
        
        # Gửi kết quả vào Kafka
        result = {
            "timestamp": datetime.now().isoformat(),
            "features": features.dict(),
            "prediction": pred,
            "probability": float(proba)
        }
        
        kafka_producer.send(OUTPUT_TOPIC, json.dumps(result).encode('utf-8'))
        logger.info(f"Sent prediction: pred={pred}, prob={proba:.3f}")
        
    except Exception as e:
        logger.exception("Error processing message")

def kafka_consumer_thread():
    logger.info("Starting Kafka consumer thread...")
    for message in kafka_consumer:
        process_message(message)

@app.on_event("startup")
async def startup_event():
    global model, kafka_consumer, kafka_producer
    
    # Load model
    if os.path.exists(MODEL_PATH):
        model = joblib.load(MODEL_PATH)
        logger.info(f"Model loaded: {MODEL_PATH}")
    else:
        logger.error(f"Model not found at {MODEL_PATH}")
    
    # Khởi tạo Kafka consumer & producer
    # Đây là cấu hình khi deploy real-time
    # kafka_consumer = KafkaConsumer(
    #     INPUT_TOPIC,
    #     bootstrap_servers=KAFKA_BOOTSTRAP,
        
    #     auto_offset_reset='latest',
    #     enable_auto_commit=True,
    #     group_id='model_service'
    # )

    # Đây là cấu hình khi test
    kafka_consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=None
    )
    
    kafka_producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=str.encode,
        value_serializer=str.encode
    )
    
    # Start consumer thread
    Thread(target=kafka_consumer_thread, daemon=True).start()

@app.on_event("shutdown")
async def shutdown_event():
    if kafka_consumer:
        kafka_consumer.close()
    if kafka_producer:
        kafka_producer.close()

# API endpoint for manual predictions (optional)
@app.post("/predict")
def predict(feat: Features):
    if model is None:
        return {"error": "Model not loaded"}
    df = pd.DataFrame([feat.dict()])
    proba = model.predict_proba(df)[:, 1][0]
    pred = int(proba > 0.5)
    return {"prediction": pred, "probability": float(proba)}
