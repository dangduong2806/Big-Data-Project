from kafka import KafkaConsumer
import requests
import json
import os
import time

import redis

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "fraud_transactions"
PREDICT_API = "http://fraud-inference:8060/predict" # sửa app thành fraud-inference

# Cấu hình Redis
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        # Thay đổi group_id để kafka consumer luôn đọc từ offset đầu tiên KHI CHƯA CÓ OFFSET MỚI
        group_id="fraud-group" + str(time.time())
    )

    for msg in consumer:
        transaction = msg.value
        res = requests.post(PREDICT_API, json=transaction)
        print(f"🔎 Transaction {transaction.get('TransactionID')} => {res.json()}")
        prediction_result = res.json()

        try:
            # GHI VÀO REDIS
            list_key = "fraud:predictions"
            # Thêm TransactionID vào kết quả để dễ tra cứu
            prediction_result['TransactionID'] = transaction.get('TransactionID')

            r.lpush(list_key, json.dumps(prediction_result))
            r.ltrim(list_key, 0, 999) # Giới hạn 1000 dự đoán mới nhất
        except Exception as e:
            print(f"❌ Không thể ghi vào Redis: {e}")

if __name__ == "__main__":
    try:
        res = requests.get(PREDICT_API)
        if res.status_code == 200:
            print("API sẵn sàng")
    except requests.exceptions.RequestException:
        print("API chưa sẵn sàng, thử lại sau 2s...")
        time.sleep(2)
    main()
