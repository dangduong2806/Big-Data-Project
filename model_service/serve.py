import os
import json
import logging
import time  # <-- Thêm time
from datetime import datetime
from typing import Dict, List
import numpy as np
from threading import Thread

import pandas as pd
import joblib
import redis  # <-- Thêm redis
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.responses import RedirectResponse, JSONResponse

from kafka import KafkaConsumer, KafkaProducer

# KHÔNG CẦN Spark ở đây nữa
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Cấu hình (Đã loại bỏ HDFS) ---
MODEL_PATH = os.environ.get("MODEL_PATH", "/models/risk_model.joblib")
FEATURES_PATH = os.environ.get("FEATURES_PATH", "/models/model_features.json")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
INPUT_TOPIC = os.environ.get("INPUT_TOPIC", "daily_prices")
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "predictions")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
WINDOW_SIZE = int(os.environ.get("WINDOW_SIZE", "21"))

app = FastAPI(title="Real-time Prediction Service (Port 8000)")

# --- (Code lớp PriceData của bạn giữ nguyên) ---
class PriceData:
    def __init__(self): self.data = []
    def add_record(self, record: Dict):
        self.data.append(record)
        if len(self.data) > WINDOW_SIZE + 1: self.data.pop(0)
    def calculate_features(self) -> Dict:
        if len(self.data) < WINDOW_SIZE + 1: return None
        df = pd.DataFrame(self.data); df['timestamp'] = pd.to_datetime(df['timestamp']); df = df.sort_values('timestamp')
        df['Daily_Return'] = df['Close'].pct_change()
        latest = df.iloc[-1]; window_for_std = df.iloc[-(WINDOW_SIZE):]
        daily_return = latest['Daily_Return']
        volatility_cluster = window_for_std['Daily_Return'].std() * (252**0.5)
        volume_based_volatility = window_for_std['Volume'].std() / window_for_std['Volume'].mean()
        if pd.isna(daily_return) or pd.isna(volatility_cluster) or pd.isna(volume_based_volatility): return None
        return {"Daily_Return": float(daily_return), "Volatility_Cluster": float(volatility_cluster), "Volume_Based_Volatility": float(volume_based_volatility)}

# --- Biến Toàn cục (Không cần Spark) ---
model = None; feature_columns = []; price_data_windows: Dict[str, PriceData] = {};
kafka_consumer = None; kafka_producer = None; redis_client = None;

# --- (Code hàm process_message của bạn giữ nguyên) ---
def process_message(msg):
    global model, feature_columns, price_data_windows, kafka_producer
    try:
        record = json.loads(msg.value.decode('utf-8')); ticker = record.get("Ticker")
        if not ticker: return
        if ticker not in price_data_windows:
            price_data_windows[ticker] = PriceData(); logger.info(f"Created new data window for ticker: {ticker}")
        window = price_data_windows[ticker]; window.add_record(record)
        base_features = window.calculate_features()
        if base_features is None: return
        if model is None or not feature_columns: logger.error("Model/features not loaded"); return
        df_predict = pd.DataFrame(columns=feature_columns); df_predict.loc[0] = 0.0
        df_predict['Daily_Return'] = base_features['Daily_Return']
        df_predict['Volatility_Cluster'] = base_features['Volatility_Cluster']
        df_predict['Volume_Based_Volatility'] = base_features['Volume_Based_Volatility']
        ticker_column_name = f"Ticker_{ticker}"
        if ticker_column_name in feature_columns: df_predict[ticker_column_name] = 1.0
        else: logger.warning(f"Ticker {ticker} not trained. Skipping."); return
        proba = model.predict_proba(df_predict[feature_columns])[:, 1][0]; pred = int(proba > 0.5)
        result = {"timestamp": datetime.now().isoformat(), "input_data_timestamp": record.get("timestamp"), "Ticker": ticker, "prediction": pred, "probability": float(proba)}
        kafka_producer.send(OUTPUT_TOPIC, json.dumps(result, default=str).encode('utf-8'))
        logger.info(f"Sent prediction to Kafka: {result}")
    except Exception as e:
        logger.exception(f"Error processing message for ticker {record.get('Ticker')}: {e}")

# --- (Code hàm kafka_consumer_thread của bạn giữ nguyên) ---
def kafka_consumer_thread():
    logger.info("Starting Kafka consumer thread...")
    for message in kafka_consumer: process_message(message)

@app.on_event("startup")
async def startup_event():
    global model, feature_columns, kafka_consumer, kafka_producer, redis_client
    
    # Tải Model & Features (Giữ nguyên)
    try:
        if os.path.exists(MODEL_PATH): model = joblib.load(MODEL_PATH); logger.info(f"Model loaded")
        else: logger.error(f"Model file not found at {MODEL_PATH}")
        if os.path.exists(FEATURES_PATH):
            with open(FEATURES_PATH, 'r') as f: feature_columns = json.load(f); logger.info(f"Feature list loaded")
        else: logger.error(f"Feature list file not found at {FEATURES_PATH}")
    except Exception as e: logger.exception(f"Error loading model or features: {e}")
        
    # Khởi tạo Kafka (Giữ nguyên)
    try:
        kafka_consumer = KafkaConsumer(INPUT_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP, auto_offset_reset='latest', group_id='model_service_multi_ticker', value_deserializer=lambda x: x)
        kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
        logger.info("Kafka consumer and producer connected.")
        Thread(target=kafka_consumer_thread, daemon=True).start()
    except Exception as e: logger.exception("Failed to connect to Kafka")

    # --- SỬA LỖI: Thêm Retry Logic cho REDIS ---
    retries = 10; delay = 5
    for i in range(retries):
        try:
            logger.info(f"Connecting to Redis at {REDIS_HOST} (Attempt {i+1}/{retries})...")
            redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
            redis_client.ping()
            logger.info(f"Connected to Redis successfully!")
            break # Kết nối thành công
        except Exception as e:
            logger.warning(f"Failed to connect to Redis: {e}")
            if i < retries - 1: logger.info(f"Retrying in {delay} seconds..."); time.sleep(delay)
            else: logger.error("Could not connect to Redis after all retries."); redis_client = None
    
    # --- LOẠI BỎ: Không cần Spark ở đây nữa ---

@app.on_event("shutdown")
async def shutdown_event():
    if kafka_consumer: kafka_consumer.close()
    if kafka_producer: kafka_producer.close()
    logger.info("Kafka/Redis resources closed.")


# --- API Endpoints (CHỈ CÁC API PREDICTION) ---
@app.get("/latest_prediction")
def get_latest_prediction():
    if redis_client is None: return JSONResponse(status_code=503, content={"error": "Redis not connected. Service is warming up."})
    latest_pred_json = redis_client.get("latest_prediction")
    if latest_pred_json: return json.loads(latest_pred_json)
    return {"message": "No prediction found in cache yet."}

@app.get("/recent_predictions")
def get_recent_predictions(limit: int = 20):
    if redis_client is None: return JSONResponse(status_code=503, content={"error": "Redis not connected. Service is warming up."})
    predictions_json_list = redis_client.lrange("recent_predictions_list", 0, limit - 1)
    if not predictions_json_list: return {"message": "No predictions found in cache yet."}
    return [json.loads(p_json) for p_json in predictions_json_list]

@app.get("/latest_prediction_all")
def get_latest_prediction_all():
    """
    Lấy dự đoán mới nhất cho TẤT CẢ các ticker.
    """
    if redis_client is None:
        return JSONResponse(status_code=503, content={"error": "Redis not connected. Service is warming up."})
    
    try:
        # 1. Tìm tất cả các key có mẫu "latest_prediction:[TICKER]"
        # 
        ticker_keys = redis_client.keys("latest_prediction:*")
        
        if not ticker_keys:
            return [] # Trả về danh sách rỗng nếu chưa có gì

        # 2. Lấy tất cả các giá trị (JSON string) của các key đó cùng lúc
        predictions_json_list = redis_client.mget(ticker_keys)
        
        # 3. Chuyển đổi chuỗi JSON thành đối tượng Python
        predictions = [json.loads(p_json) for p_json in predictions_json_list if p_json]
        
        # Sắp xếp theo Ticker để đảm bảo thứ tự ổn định
        predictions.sort(key=lambda x: x.get('Ticker', ''))
        
        return predictions
            
    except Exception as e:
        logger.exception("Error reading all ticker keys from Redis")
        return JSONResponse(status_code=500, content={'error': str(e)})
# --- LOẠI BỎ: Tất cả các API HDFS/Spark ---
# (Đã xóa /available_tickers, /historical_data, /ticker_summary)

# --- PHỤC VỤ UI (Giữ nguyên) ---
app.mount("/ui", StaticFiles(directory="ui"), name="ui")
@app.get("/")
async def read_root():
    return RedirectResponse(url="/ui/index.html")