import os
import json
import logging
import time  # <-- THÊM IMPORT TIME
import redis
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
INPUT_TOPIC = os.environ.get("INPUT_TOPIC", "predictions")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")

# --- THÊM KHỐI TRY/EXCEPT CHO REDIS ---
try:
    logger.info(f"Connecting to Redis at {REDIS_HOST}...")
    r = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
    r.ping()
    logger.info("Connected to Redis successfully!")
except Exception as e:
    logger.exception(f"Could not connect to Redis at {REDIS_HOST}. Exiting.")
    exit(1)

# --- THÊM VÒNG LẶP RETRY CHO KAFKA ---
consumer = None
retries = 10
delay = 5
for i in range(retries):
    try:
        logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP} (Attempt {i+1}/{retries})...")
        consumer = KafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset='latest', # Chỉ đọc message mới
            group_id='prediction_savers_group', # Dùng group_id ổn định
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logger.info("Connected to Kafka successfully!")
        break  # Thoát vòng lặp nếu kết nối thành công
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        if i < retries - 1:
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
        else:
            logger.error("Could not connect to Kafka after all retries. Exiting.")
            exit(1) # Thoát nếu không thể kết nối

# --- CHẠY VÒNG LẶP CONSUMER ---
try:
    logger.info("Waiting for predictions...")
    for message in consumer:
        prediction_data = message.value
        
        # Dùng timestamp làm key
        key = prediction_data.get('timestamp')
        if key:
            # Lưu toàn bộ JSON của dự đoán vào Redis
            r.set(key, json.dumps(prediction_data))
            
            # Lưu 1 key đặc biệt để lấy dự đoán MỚI NHẤT
            r.set('latest_prediction', json.dumps(prediction_data))
            
            logger.info(f"Saved prediction for {prediction_data.get('Ticker')} ({key}) to Redis")

except Exception as e:
    logger.exception("Error in Kafka consumer or Redis write")
finally:
    if consumer:
        consumer.close()