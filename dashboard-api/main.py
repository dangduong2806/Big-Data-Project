import os
import json
import redis
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import datetime

# Khởi tạo FastAPI App
app = FastAPI(title="Dashboard API")

# Cấu hình CORS (quan trọng)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Cho phép tất cả
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Kết nối tới Redis
# Chúng ta dùng "redis" làm host vì nó là tên service trong docker-compose
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
try:
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=False)
    r.ping()
    print(f"✅ Kết nối thành công tới Redis tại {REDIS_HOST}")
except Exception as e:
    print(f"❌ Không thể kết nối Redis: {e}")
    r = None

# Danh sách các mã chứng khoán (từ project của bạn)
COMPANIES = ["GSPC", "XOM", "CVX", "BP"]

# === CÁC API ENDPOINTS ===

@app.get("/")
async def get_dashboard():
    """Phục vụ file index.html tĩnh"""
    return FileResponse("static/index.html")

@app.get("/api/market-risk")
async def get_market_risk_data():
    """Lấy dự đoán MỚI NHẤT cho mỗi mã chứng khoán"""
    predictions = []
    if not r:
        return {"error": "Redis not connected"}
        
    for company in COMPANIES:
        try:
            # Lấy 1 mục mới nhất (lindex 0) từ list của mỗi công ty
            key = f"predictions:{company}"
            latest_pred_json = r.lindex(key, 0) # Lấy mục đầu tiên
            
            if latest_pred_json:
                pred_data = json.loads(latest_pred_json.decode('utf-8'))
                predictions.append(pred_data)
            else:
                # Nếu không có dữ liệu, trả về cấu trúc rỗng
                predictions.append({
                    "Company": company,
                    "Prediction": 0,
                    "Probability": 0.0,
                    "Daily_Return": 0.0,
                    "Volatility_Cluster": 0.0,
                    "Volume_Based_Volatility": 0.0,
                    "Date": "N/A"
                })
        except Exception as e:
            print(f"Lỗi khi đọc key {key}: {e}")
            
    return predictions

@app.get("/api/fraud")
async def get_fraud_data():
    """Lấy 10 cảnh báo gian lận mới nhất"""
    if not r:
        return {"error": "Redis not connected"}

    try:
        # Lấy 10 mục mới nhất từ list 'fraud:predictions'
        fraud_data_json = r.lrange("fraud:predictions", 0, 9)
        
        # Parse JSON
        fraud_data = [json.loads(item.decode('utf-8')) for item in fraud_data_json]
        
        # Chuyển đổi định dạng cho dashboard
        formatted_data = []
        for item in fraud_data:
            formatted_data.append({
                "transactionId": item.get("TransactionID", "N/A"),
                "amount": float(item.get("TransactionAmt", 0)), # Giả sử bạn có TransactionAmt
                "isFraud": int(item.get("isFraud", 0)),
                "probability": float(item.get("fraud_probability", 0)),
                
                # === ĐÂY LÀ DÒNG ĐÃ SỬA ===
                # Cung cấp thời gian UTC hiện tại (dạng ISO) nếu 'timestamp' không tồn tại
                "timestamp": item.get("timestamp", datetime.datetime.now(datetime.timezone.utc).isoformat()) 
            })
        return formatted_data
    except Exception as e:
        print(f"Lỗi khi đọc 'fraud:predictions': {e}")
        return []