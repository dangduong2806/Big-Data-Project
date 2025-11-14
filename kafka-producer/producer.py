import json
import time
import os
import logging
import pandas as pd
from datetime import datetime

import yfinance as yf
from kafka import KafkaProducer

import threading


LOG = logging.getLogger("producer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# Config via env vars for flexibility in docker-compose
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
HISTORICAL_TOPIC = os.getenv("HISTORICAL_TOPIC_GSPC", "historical_prices_GSPC")
HISTORICAL_TOPIC_XOM = os.getenv("HISTORICAL_TOPIC_XOM","historical_prices_XOM")
HISTORICAL_TOPIC_CVX = os.getenv("HISTORICAL_TOPIC_CVX","historical_prices_CVX")
HISTORICAL_TOPIC_BP = os.getenv("HISTORICAL_TOPIC_BP", "historical_prices_BP")

DAILY_TOPIC = os.getenv("DAILY_TOPIC_GSPC", "daily_prices_GSPC")
DAILY_TOPIC_XOM = os.getenv("DAILY_TOPIC_XOM", 'daily_prices_XOM')
DAILY_TOPIC_CVX = os.getenv("DAILY_TOPIC_CVX", "daily_prices_CVX")
DAILY_TOPIC_BP = os.getenv("DAILY_TOPIC_BP", "daily_prices_BP")

TICKER = os.getenv("TICKER", "^GSPC")
TICKER_XOM = os.getenv("TICKER_XOM", "XOM")
TICKER_CVX = os.getenv("TICKER_CVX", "CVX")
TICKER_BP = os.getenv("TICKER_BP", "BP")

companies = {"GSPC":[TICKER, HISTORICAL_TOPIC, DAILY_TOPIC],
             "XOM":[TICKER_XOM, HISTORICAL_TOPIC_XOM, DAILY_TOPIC_XOM], 
             "CVX":[TICKER_CVX, HISTORICAL_TOPIC_CVX, DAILY_TOPIC_CVX], 
             "BP":[TICKER_BP, HISTORICAL_TOPIC_BP, DAILY_TOPIC_BP]}

# Set polling interval in seconds
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
PERSIST_LAST_FILE = os.getenv("LAST_SENT_FILE", "/app/last_sent.json")


# Kafka producer with better throughput settings (async sends, batching)
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    compression_type="gzip",
    linger_ms=100,
    batch_size=16384,
)


def load_last_sent():
    try:
        if os.path.exists(PERSIST_LAST_FILE):
            with open(PERSIST_LAST_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return datetime.fromisoformat(data.get("last_sent")) if data.get("last_sent") else None
    except Exception:
        LOG.exception("Failed to load last sent timestamp")
    return None


def save_last_sent(ts: datetime):
    try:
        with open(PERSIST_LAST_FILE, "w", encoding="utf-8") as f:
            json.dump({"last_sent": ts.isoformat()}, f)
    except Exception:
        LOG.exception("Failed to persist last sent timestamp")

def send_historical_data(company: str):
    """Gửi dữ liệu lịch sử 1 năm lên Kafka"""
    stastics = companies[company]
    TICKER = stastics[0]
    HIS_TOPIC = stastics[1]

    LOG.info("Fetching historical data for %s...", TICKER)
    ticker = yf.Ticker(TICKER)
    
    # Lấy dữ liệu lịch sử 1 năm
    historical_data = ticker.history(period="1y")
    
    if historical_data is None or historical_data.empty:
        LOG.error("Failed to fetch historical data")
        return
    
    # Xử lý và gửi từng record
    for i, (idx, row) in enumerate(historical_data.iterrows()):
        record = {
            # Thêm dòng này
            "Company": company,
            "timestamp": idx.isoformat(),
            "Open": float(row["Open"]),
            "High": float(row["High"]),
            "Low": float(row["Low"]),
            "Close": float(row["Close"]),
            "Volume": int(row["Volume"]),
        }
        
        # Gửi lên Kafka
        producer.send(HIS_TOPIC, 
                     key=idx.isoformat().encode(),
                     value=record)
        
        # Thêm delay nhẹ để tránh Yahoo finance/ Kafka bị nghẽn
        if i % 100 == 0:
            producer.flush()
            time.sleep(0.1)

    producer.flush()
    LOG.info("Historical data of %s sent successfully", company)
            

def fetch_and_send(company: str):   
    stastics = companies[company]
    TICKER = stastics[0]
    DAILY_TOPIC = stastics[2]

    last_sent = load_last_sent()
    LOG.info("Starting polling for %s, poll interval=%ss, bootstrap=%s", TICKER, POLL_INTERVAL, BOOTSTRAP_SERVERS)

    ticker = yf.Ticker(TICKER)
    
    sent_count = 0
    new_last = None
    try:    
        while True:
            try:
                # fetch recent intraday data (5 days, 2m interval). yfinance may throttle; adjust interval accordingly
                hist = ticker.history(period="1d", interval="1m")
            except Exception:
                LOG.exception("yfinance fetch failed, retrying after sleep")
                time.sleep(POLL_INTERVAL)
                continue

            if hist is None or hist.empty:
                LOG.debug("No data returned from yfinance")
                time.sleep(POLL_INTERVAL)
                continue

            # reset index: index is timestamp
            hist = hist.reset_index()

            # Ensure consistent column names
            # yfinance columns: Datetime, Open, High, Low, Close, Volume
            if "Datetime" in hist.columns:
                time_col = "Datetime"
            elif "Date" in hist.columns:
                time_col = "Date"
            else:
                time_col = hist.columns[0]

            # Chỉ xử lý dữ liệu mới nhất
            if not hist.empty:
                latest_row = hist.iloc[-1]
                ts = latest_row[time_col]
                
                # Convert timestamp
                if isinstance(ts, str):
                    ts_dt = datetime.fromisoformat(ts)
                else:
                    ts_dt = ts.to_pydatetime()

                if last_sent is not None and ts_dt <= last_sent:
                    time.sleep(POLL_INTERVAL)
                    continue

                record = {
                    "Date": ts_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "Company": TICKER.replace("^", ""),
                    "Open": float(latest_row.get("Open", 0.0)),
                    "High": float(latest_row.get("High", 0.0)),
                    "Low": float(latest_row.get("Low", 0.0)),
                    "Close": float(latest_row.get("Close", 0.0)),
                    "Volume": int(latest_row.get("Volume", 0) or 0),
                }

                # async send
                # kafka ghi đè message nếu key trùng lặp
                producer.send(DAILY_TOPIC, key=ts_dt.isoformat().encode(), value=record)
                
                # Thêm phần này
                producer.flush() # Đảm bảo kafka nhận message ngày
                LOG.info("Sent record to daily_prices: %s", record)

                sent_count += 1
                new_last = ts_dt

            # update last_sent if we sent new records
            if new_last and (last_sent is None or new_last > last_sent):
                last_sent = new_last
                save_last_sent(last_sent)

            # flush remaining
            producer.flush()

            LOG.info("Polling iteration done. total_sent=%d last_sent=%s", sent_count, last_sent)
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        LOG.info("Stopping on user interrupt")
    producer.flush()
    
        
if __name__ == "__main__":
    # Thực hiện đẩy historical data lên trước để train
    for company in companies.keys():
        send_historical_data(company)
        
    # Sau đó mới cập nhật dữ liệu mới liên tục từ yahoo finance
    threads = []
    for company in companies.keys():
        t = threading.Thread(target=fetch_and_send, args=(company,), daemon=True)
        t.start()
        threads.append(t)
    
    # for t in threads:
    #     t.join()
