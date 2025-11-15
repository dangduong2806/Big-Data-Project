import json
import time
import os
import logging
import pandas as pd
from datetime import datetime

import yfinance as yf
from kafka import KafkaProducer

LOG = logging.getLogger("producer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# --- 1. THAY ĐỔI: Danh sách các tickers chúng ta muốn theo dõi ---
TICKERS = os.getenv("TICKERS", "^GSPC,AAPL,MSFT,GOOG").split(',')

# Config via env vars
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
HISTORICAL_TOPIC = os.getenv("HISTORICAL_TOPIC", "historical_prices")
DAILY_TOPIC = os.getenv("DAILY_TOPIC", "daily_prices")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

# Kafka producer với retry logic (Giữ nguyên)
producer = None
retries = 10
delay = 5
for i in range(retries):
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            compression_type="gzip",
            linger_ms=100,
            batch_size=16384,
        )
        LOG.info("Kafka Producer connected successfully!")
        break
    except Exception as e:
        LOG.error(f"Failed to connect to Kafka (Attempt {i+1}/{retries}): {e}")
        if i < retries - 1:
            LOG.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
        else:
            LOG.error("Could not connect to Kafka after all retries. Exiting.")
            exit(1)

def send_historical_data():
    """Gửi dữ liệu lịch sử 1 năm (THÔ) của TẤT CẢ tickers lên Kafka"""
    
    for ticker_symbol in TICKERS:
        LOG.info(f"Fetching historical data for {ticker_symbol}...")
        ticker_obj = yf.Ticker(ticker_symbol)
        
        historical_data = ticker_obj.history(period="1y")
        
        if historical_data is None or historical_data.empty:
            LOG.error(f"Failed to fetch historical data for {ticker_symbol}")
            continue # Bỏ qua ticker này, tiếp tục với ticker khác

        # Gửi dữ liệu thô, THÊM CỘT 'Ticker'
        for idx, row in historical_data.iterrows():
            record = {
                "timestamp": idx.isoformat(),
                "Ticker": ticker_symbol,  # <-- 2. THAY ĐỔI: Thêm cột Ticker
                "Open": float(row["Open"]),
                "High": float(row["High"]),
                "Low": float(row["Low"]),
                "Close": float(row["Close"]),
                "Volume": int(row["Volume"]),
            }
            
            producer.send(HISTORICAL_TOPIC, 
                         key=f"{ticker_symbol}_{idx.isoformat()}".encode(),
                         value=record)
        
        producer.flush()
        LOG.info(f"Historical data for {ticker_symbol} sent successfully")

def fetch_and_send():
    """Lấy dữ liệu mới nhất (streaming) cho TẤT CẢ tickers"""
    
    LOG.info(f"Starting polling for {TICKERS}, poll interval={POLL_INTERVAL}s")

    # Tạo một đối tượng yf.Tickers (số nhiều) để lấy dữ liệu hiệu quả
    ticker_objs = yf.Tickers(TICKERS)
    
    # Gửi dữ liệu lịch sử một lần duy nhất khi khởi động
    send_historical_data()
    
    LOG.info("Historical data push complete. Starting real-time polling loop...")
    
    try:    
        while True:
            try:
                # 3. THAY ĐỔI: Lấy dữ liệu 1 ngày cho TẤT CẢ tickers cùng lúc
                hist_data_all = ticker_objs.history(period="1d", interval="1m")
                
                if hist_data_all is None or hist_data_all.empty:
                    LOG.debug("No data returned from yfinance")
                    time.sleep(POLL_INTERVAL)
                    continue

                # Lấy dữ liệu hàng cuối cùng (mới nhất) cho mỗi ticker
                latest_rows = hist_data_all.iloc[-1]
                
                for ticker_symbol in TICKERS:
                    if ticker_symbol not in latest_rows['Close']:
                        continue # Bỏ qua nếu ticker này không có dữ liệu mới

                    latest_data = latest_rows.xs(ticker_symbol, level=1)
                    ts_dt = latest_rows.name.to_pydatetime()

                    # 4. THAY ĐỔI: Tạo record với schema THỐNG NHẤT
                    record = {
                        "timestamp": ts_dt.isoformat(),
                        "Ticker": ticker_symbol, 
                        "Open": float(latest_data.get("Open", 0.0)),
                        "High": float(latest_data.get("High", 0.0)),
                        "Low": float(latest_data.get("Low", 0.0)),
                        "Close": float(latest_data.get("Close", 0.0)),
                        "Volume": int(latest_data.get("Volume", 0) or 0),
                    }

                    producer.send(DAILY_TOPIC, 
                                 key=f"{ticker_symbol}_{ts_dt.isoformat()}".encode(), 
                                 value=record)
                    LOG.info(f"Sent record to daily_prices: {record}")

                producer.flush()
                LOG.info(f"Polling iteration done. Sleeping for {POLL_INTERVAL}s")
                time.sleep(POLL_INTERVAL)

            except Exception as e:
                LOG.exception(f"Error in polling loop: {e}. Retrying after sleep...")
                time.sleep(POLL_INTERVAL)
                
    except KeyboardInterrupt:
        LOG.info("Stopping on user interrupt")
        
if __name__ == "__main__":
    fetch_and_send()