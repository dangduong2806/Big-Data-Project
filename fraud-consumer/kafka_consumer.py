from kafka import KafkaConsumer
import requests
import json
import os
import time  # <-- Thêm import

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "fraud_transactions"
PREDICT_API = "http://fraud-api:8060/predict" # <-- Sửa tên service cho rõ ràng

def main():
    consumer = None
    retries = 10
    delay = 5

    # --- THÊM VÒNG LẶP RETRY ---
    for i in range(retries):
        try:
            print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS} (Attempt {i+1}/{retries})...")
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[BOOTSTRAP_SERVERS],
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id="fraud-group"
            )
            print("✅ Kafka Consumer connected successfully!")
            break # Thoát vòng lặp
        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            if i < retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("❌ Could not connect to Kafka after all retries. Exiting.")
                exit(1)
    # --- KẾT THÚC VÒNG LẶP ---

    print("Waiting for fraud transactions...")
    for msg in consumer:
        transaction = msg.value
        try:
            res = requests.post(PREDICT_API, json=transaction)
            print(f"🔎 Transaction {transaction.get('TransactionID', 'N/A')} => {res.json()}")
        except requests.exceptions.ConnectionError:
            print(f"❌ Failed to connect to Fraud API at {PREDICT_API}. Is 'fraud-api' running?")
            time.sleep(5) # Chờ API khởi động
        except Exception as e:
            print(f"Error processing message: {e}")

if __name__ == "__main__":
    main()