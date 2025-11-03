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


# Config via env vars for flexibility in docker-compose
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("TOPIC", "raw_prices")
TICKER = os.getenv("TICKER", "^GSPC")
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


def calculate_features(current_data, historical_data):
    """Tính toán features dựa trên dữ liệu hiện tại và lịch sử"""
    # Ghép dữ liệu hiện tại vào historical_data
    df = historical_data.append(current_data)
    
    # Tính toán các features
    df['Daily_Return'] = df['Close'].pct_change()
    df['Volatility_Cluster'] = df['Daily_Return'].rolling(21).std() * (252**0.5)
    df['Volume_Based_Volatility'] = df['Volume'].rolling(21).std() / df['Volume'].rolling(21).mean()
    
    # Lấy giá trị cuối cùng (dữ liệu hiện tại)
    latest = df.iloc[-1]
    
    return {
        'Daily_Return': float(latest['Daily_Return']) if not pd.isna(latest['Daily_Return']) else 0.0,
        'Volatility_Cluster': float(latest['Volatility_Cluster']) if not pd.isna(latest['Volatility_Cluster']) else 0.0,
        'Volume_Based_Volatility': float(latest['Volume_Based_Volatility']) if not pd.isna(latest['Volume_Based_Volatility']) else 0.0
    }

def fetch_and_send():
    last_sent = load_last_sent()
    LOG.info("Starting polling for %s, poll interval=%ss, bootstrap=%s", TICKER, POLL_INTERVAL, BOOTSTRAP_SERVERS)

    ticker = yf.Ticker(TICKER)
    
    # Lấy dữ liệu lịch sử cho việc tính features
    historical_data = ticker.history(period="1mo")
    
    sent_count = 0
    try:
        while True:
            try:
                # fetch recent intraday data (1 day, 1m interval). yfinance may throttle; adjust interval accordingly
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

                # Chuẩn bị dữ liệu cơ bản
                current_data = pd.Series({
                    'Open': float(latest_row['Open']),
                    'High': float(latest_row['High']),
                    'Low': float(latest_row['Low']),
                    'Close': float(latest_row['Close']),
                    'Volume': float(latest_row['Volume'])
                })

                # Tính toán features
                features = calculate_features(current_data, historical_data)
                
                # Chuẩn bị record để gửi
                record = {
                    'timestamp': ts_dt.isoformat(),
                    'Open': float(latest_row['Open']),
                    'High': float(latest_row['High']),
                    'Low': float(latest_row['Low']),
                    'Close': float(latest_row['Close']),
                    'Volume': float(latest_row['Volume']),
                    **features  # Thêm các features đã tính toán
                }

                record = {
                    "Date": ts_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "Company": TICKER.replace("^", ""),
                    "Open": float(row.get("Open", 0.0)),
                    "High": float(row.get("High", 0.0)),
                    "Low": float(row.get("Low", 0.0)),
                    "Close": float(row.get("Close", 0.0)),
                    "Volume": int(row.get("Volume", 0) or 0),
                }

                # async send
                producer.send(TOPIC, record)
                sent_count += 1
                batch_counter += 1
                new_last = ts_dt

                # flush periodically to ensure delivery and limit memory
                if batch_counter >= 100:
                    producer.flush()
                    batch_counter = 0

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
    finally:
        try:
            producer.flush()
            producer.close()
        except Exception:
            pass


if __name__ == "__main__":
    fetch_and_send()
