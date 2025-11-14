from kafka import KafkaConsumer
import requests
import json
import os

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "fraud_transactions"
PREDICT_API = "http://app:8060/predict"

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id="fraud-group"
    )

    for msg in consumer:
        transaction = msg.value
        res = requests.post(PREDICT_API, json=transaction)
        print(f"🔎 Transaction {transaction.get('TransactionID')} => {res.json()}")

if __name__ == "__main__":
    main()
