from kafka import KafkaProducer
import pandas as pd
import json
import time
import os
import numpy as np

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "fraud_transactions"
TEST_DATA_PATH = os.getenv("TEST_DATA_PATH", "/app/data/test_transaction.csv")

def main():
    producer = None
    retries = 10
    delay = 5

    # --- THÊM VÒNG LẶP RETRY ---
    for i in range(retries):
        try:
            print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS} (Attempt {i+1}/{retries})...")
            producer = KafkaProducer(
                bootstrap_servers=[BOOTSTRAP_SERVERS],
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("✅ Kafka Producer connected successfully!")
            break  # Thoát vòng lặp nếu kết nối thành công
        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            if i < retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("❌ Could not connect to Kafka after all retries. Exiting.")
                exit(1) # Thoát nếu không thể kết nối
    # --- KẾT THÚC VÒNG LẶP ---

    print(f"Reading data from {TEST_DATA_PATH}...")
    df = pd.read_csv(TEST_DATA_PATH)
    
    for _, row in df.iterrows():
        record = row.replace({np.nan: None, np.inf: None, -np.inf: None}).to_dict()
        producer.send(TOPIC, record)
        print(f"✅ Sent: {record.get('TransactionID', 'N/A')}")
        time.sleep(1)  # mô phỏng gửi realtime
    
    producer.flush()
    print("✅ All test data sent.")

if __name__ == "__main__":
    main()