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
    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    df = pd.read_csv(TEST_DATA_PATH)
    for _, row in df.iterrows():
        # record = row.to_dict()
        record = row.replace({np.nan: None, np.inf: None, -np.inf: None}).to_dict() # thêm dòng này
        producer.send(TOPIC, record)
        print(f"✅ Sent: {record['TransactionID']}")
        time.sleep(1)  # mô phỏng gửi realtime

if __name__ == "__main__":
    main()
