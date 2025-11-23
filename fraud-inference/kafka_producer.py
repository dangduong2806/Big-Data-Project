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

    chunksize = 1000
    for chunk in pd.read_csv(TEST_DATA_PATH, chunksize=chunksize):
        # xử lý NaN / Inf
        chunk = chunk.replace({np.nan: None, np.inf: None, -np.inf: None})
        
        # gửi từng dòng trong chunk
        for _, row in chunk.iterrows():
            record = row.to_dict()
            producer.send(TOPIC, record)
            print(f"✅ Sent: {record['TransactionID']}")
        
        producer.flush()  # đảm bảo tất cả message trong chunk được gửi
if __name__ == "__main__":
    main()
