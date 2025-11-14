import os
import json
import logging
from datetime import datetime
from threading import Thread

from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaConsumer, KafkaProducer

from pyspark.sql import SparkSession, Row
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

import pandas as pd
from collections import deque
import math
import threading
import uvicorn

# CẤU HÌNH LOGGING
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("RealTimeRiskModel")

# CẤU HÌNH ENVIRONMENT
# Chú ý đoạn này
COMPANY = os.getenv("COMPANY")
PORT = int(os.getenv("PORT"))
MODEL_PATH = f"hdfs://namenode:9000/models/{COMPANY}_risk_model"
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
INPUT_TOPIC = f"daily_prices_{COMPANY}"
# Chú ý đoạn này
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "predictions")

WINDOW_SIZE = 3

# KHỞI TẠO SPARK SESSION
spark = SparkSession.builder \
    .appName(f"realtime-risk-{COMPANY}") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

#  KHAI BÁO SCHEMA CHO FEATURES
feature_schema = StructType([
    StructField("Date", TimestampType(), True),
    StructField("Company", StringType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", LongType(), True),
    StructField("Daily_Return", DoubleType(), True),
    StructField("Volatility_Cluster", DoubleType(), True),
    StructField("Volume_Based_Volatility", DoubleType(), True)
])

class Features(BaseModel):
    Daily_Return: float
    Volatility_Cluster: float
    Volume_Based_Volatility: float

# LOAD MODEL MLlib TỪ HDFS
logger.info(f"Loading MLlib model for {COMPANY} from HDFS: {MODEL_PATH}")
try:
    model = PipelineModel.load(MODEL_PATH)
    logger.info(f"✅ Model for {COMPANY} loaded successfully")
except Exception as e:
    model = None
    logger.error(f"❌ Failed to load model from {MODEL_PATH}: {e}")

# XỬ LÝ DỮ LIỆU KAFKA REALTIME

buffer_lock = threading.Lock()
buffer = deque(maxlen=WINDOW_SIZE)

# ========== Tính đặc trưng rủi ro ==========
def compute_risk_features(df_pd):
    """
    df_pd: pandas DataFrame với ít nhất 3 dòng gần nhất
    Trả về Spark DataFrame có đặc trưng rủi ro.
    """
    df_pd["Date"] = pd.to_datetime(df_pd["Date"])
    df_pd = df_pd.sort_values("Date").reset_index(drop=True)

    # 1️⃣ Daily Return (chỉ tính cho bản ghi mới nhất)
    df_pd["Daily_Return"] = df_pd["Close"].pct_change()

    # 2️⃣ Volatility Cluster (annualized std của Daily_Return trong window)
    df_pd['Volatility_Cluster'] = df_pd["Daily_Return"].std() * math.sqrt(252)

    # 3️⃣ Volume-Based Volatility (std(volume)/mean(volume))
    df_pd['Volume_Based_Volatility'] = df_pd["Volume"].std() / df_pd["Volume"].mean()

    df_pd = df_pd.fillna(0)
    return spark.createDataFrame(df_pd, schema=feature_schema)

# ========== Hàm dự đoán ==========
def predict_from_buffer():
    df_pd = pd.DataFrame(list(buffer))
    if len(df_pd) < WINDOW_SIZE:
        return None  # chưa đủ dữ liệu
    
    df_spark = compute_risk_features(df_pd)

    assembler = VectorAssembler(
        inputCols=["Daily_Return", "Volatility_Cluster", 'Volume_Based_Volatility'],
        outputCol="Features"
    )

    df_vec = assembler.transform(df_spark)
    # Chú ý đoạn select
    preds = model.transform(df_vec).select(
        "Date", 
        "Daily_Return",
        "Volatility_Cluster",
        "Volume_Based_Volatility",
        "prediction", 
        "probability"
    ).collect()
    if preds:
        last = preds[-1]
        pred = int(last["prediction"])
        proba = float(last["probability"][1])
        ts = last["Date"]
        print(f"[{COMPANY}] {ts} → Pred: {pred}, Prob: {proba:.4f}")
        return {"Company": COMPANY, "Date": ts, "Prediction": pred, "Probability": proba,
                "Daily_Return": float(last["Daily_Return"]),
                "Volatility_Cluster": float(last["Volatility_Cluster"]),
                "Volume_Based_Volatility": float(last["Volume_Based_Volatility"])}
    
    return None

# ========== Kafka Consumer ==========
def safe_append(record):
    with buffer_lock:
        buffer.append(record)
def safe_predict():
    with buffer_lock:
        return predict_from_buffer()
    
def consume_kafka():
    # để tạm là earliest cho test
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )
    print(f"Listening to topic {INPUT_TOPIC} for company {COMPANY}...")

    for msg in consumer:
        record = msg.value
        if record.get("Company") != COMPANY:
            continue
        
        # buffer.append(record)
        # result = predict_from_buffer()
        safe_append(record)
        result = safe_predict()
        if result:
            # Lưu result lên Redis
            print("✅ Realtime Prediction:", result)

threading.Thread(target=consume_kafka, daemon=True).start()


# ========== FastAPI ==========
app = FastAPI(title=f"{COMPANY} Realtime Prediction API")

@app.get("/")
def root():
    return {"status": "running", "company": COMPANY, "window_size": WINDOW_SIZE}

@app.post("/predict_once")
def predict_once(records: list):
    """ Dự đoán thủ công cho 1 list record (ít nhất 3 record gần nhất) """
    try:
        for r in records:
            # buffer.append(r)
            safe_append(r)
        # result = predict_from_buffer()
        result = safe_predict()
        return result or {"error": "Not enough data"}
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)