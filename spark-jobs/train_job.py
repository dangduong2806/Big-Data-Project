import os
import logging
import pandas as pd
from pyspark.sql import SparkSession
import joblib
import json  # <-- Thêm import json
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    classification_report,
    average_precision_score,
    confusion_matrix,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("train_job")

# --- 1. Định nghĩa đường dẫn ---
TRAIN_PARQUET_PATH = "hdfs://namenode:9000/data/raw/stock_data"
MODEL_SAVE_PATH = os.environ.get('MODEL_SAVE_PATH', '/models/risk_model.joblib')
# --- THÊM MỚI: Đường dẫn để lưu các cột feature đã học ---
FEATURES_SAVE_PATH = os.environ.get('FEATURES_SAVE_PATH', '/models/model_features.json')

spark = SparkSession.builder \
    .appName("train-risk-model-multi") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

def compute_features(df):
    """Hàm tính toán feature cho từng nhóm Ticker"""
    df = df.sort_values('timestamp') # Sắp xếp theo thời gian
    
    # Tính toán rolling features
    df['Daily_Return'] = df['Close'].pct_change()
    df['Volatility_Cluster'] = df['Daily_Return'].rolling(21).std() * (252**0.5)
    df['Volume_Based_Volatility'] = df['Volume'].rolling(21).std() / df['Volume'].rolling(21).mean()
    
    # Tính toán label (mục tiêu)
    threshold = -0.03
    window = 5
    for i in range(1, window + 1):
        df[f'{i}d_ret'] = df['Close'].shift(-i) / df['Close'] - 1
    df['Future_Min_Return'] = df[[f'{i}d_ret' for i in range(1, window + 1)]].min(axis=1)
    df['Risk_Event'] = (df['Future_Min_Return'] <= threshold).astype(int)
    
    return df

try:
    # --- 2. Đọc dữ liệu từ HDFS ---
    logger.info(f"Reading training data from {TRAIN_PARQUET_PATH}...")
    df_spark = spark.read.parquet(TRAIN_PARQUET_PATH)
    
    # Chuyển sang Pandas
    df = df_spark.toPandas()
    
    if df.empty:
        raise ValueError("Dataframe is empty after loading from HDFS.")
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    logger.info(f"Data loaded with {len(df)} rows and {len(df['Ticker'].unique())} unique tickers.")

    # --- 3. Tính toán Feature (Nâng cấp) ---
    logger.info("Computing features (groupby Ticker)...")
    # Sử dụng groupby().apply() để tính rolling feature cho TỪNG ticker RIÊNG BIỆT
    # 
    df_features = df.groupby('Ticker').apply(compute_features)
    df_features = df_features.reset_index(drop=True) # Reset index sau khi groupby

    # --- 4. Biến đổi Cột 'Ticker' (One-Hot Encoding) ---
    logger.info("Performing one-hot encoding on 'Ticker' column...")
    df_final = pd.get_dummies(df_features, columns=['Ticker'], prefix='Ticker')

    # --- 5. Chuẩn bị dữ liệu Training ---
    base_features = ['Daily_Return', 'Volatility_Cluster', 'Volume_Based_Volatility']
    # Lấy danh sách các cột Ticker mới được tạo (ví dụ: Ticker_AAPL, Ticker_MSFT...)
    ticker_features = [col for col in df_final.columns if col.startswith('Ticker_')]
    
    feature_cols = base_features + ticker_features
    target_col = 'Risk_Event'
    
    # Loại bỏ các hàng NaN (do rolling(21) và shift(-5) tạo ra)
    cols_to_check = feature_cols + [target_col]
    df_final = df_final.dropna(subset=cols_to_check)
    
    X = df_final[feature_cols]
    y = df_final[target_col]

    if X.empty or y.empty:
        raise ValueError("Dataframe is empty after feature engineering and dropna().")

    logger.info(f"Training with {len(X)} samples and {len(feature_cols)} features.")
    
    # Chia dữ liệu (dùng stratify=y vì dữ liệu risk thường mất cân bằng)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, shuffle=True, stratify=y, random_state=42
    )

    # --- 6. Huấn luyện Model ---
    logger.info("Training Random Forest model...")
    model = RandomForestClassifier(
        n_estimators=100, 
        max_depth=10, 
        random_state=42, 
        class_weight='balanced',
        n_jobs=-1
    )
    model.fit(X_train, y_train)

    # --- 7. Đánh giá Model ---
    logger.info("Evaluating model on test set...")
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]
    pr_auc = average_precision_score(y_test, y_proba)
    logger.info(f"Test Precision-Recall AUC (PR AUC): {pr_auc:.4f}")
    logger.info(f"Classification Report:\n{classification_report(y_test, y_pred, zero_division=0)}")

    # --- 8. Lưu Model VÀ Features ---
    # Lưu model
    os.makedirs(os.path.dirname(MODEL_SAVE_PATH), exist_ok=True)
    logger.info(f"Saving model to {MODEL_SAVE_PATH}")
    joblib.dump(model, MODEL_SAVE_PATH)
    
    # Lưu danh sách các cột feature (RẤT QUAN TRỌNG)
    logger.info(f"Saving feature list to {FEATURES_SAVE_PATH}")
    with open(FEATURES_SAVE_PATH, 'w') as f:
        json.dump(feature_cols, f)
    
    logger.info("Model training and saving completed successfully")

except Exception as e:
    logger.exception(f"Failed during model training or saving: {e}")
    raise

finally:
    spark.stop()