import os
import logging
import pandas as pd
from pyspark.sql import SparkSession
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    classification_report,
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    confusion_matrix,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("train_job")

# Configuration: allow overriding HDFS path via env var. Try processed path first,
# then fallback to raw path where the streaming job writes features.
TRAIN_PARQUET_PATH = os.environ.get(
    "TRAIN_PARQUET_PATH",
    "hdfs://namenode:9000/data/processed"
)
FALLBACK_PARQUET_PATH = "hdfs://namenode:9000/data/raw/stock_data"

spark = SparkSession.builder \
    .appName("train-risk-model") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Read parquet from HDFS; try primary path then fallback
def read_parquet_with_fallback(primary, fallback):
    try:
        logger.info("Reading training data from %s", primary)
        return spark.read.parquet(primary)
    except Exception as e:
        logger.warning("Failed to read %s: %s", primary, e)
        logger.info("Trying fallback path %s", fallback)
        return spark.read.parquet(fallback)


df_spark = read_parquet_with_fallback(TRAIN_PARQUET_PATH, FALLBACK_PARQUET_PATH)
df = df_spark.toPandas()

# Normalize time column: accept 'Date' or 'timestamp'
if 'Date' in df.columns:
    time_col = 'Date'
elif 'timestamp' in df.columns:
    time_col = 'timestamp'
else:
    # fallback to first column if it looks like a datetime index
    time_col = df.columns[0]

# ensure datetime and sort
try:
    df[time_col] = pd.to_datetime(df[time_col])
    df = df.sort_values(time_col)
except Exception:
    logger.warning("Could not parse/convert time column %s; proceeding without sorting", time_col)

# Feature columns that streaming producer already writes
feature_cols = ['Daily_Return', 'Volatility_Cluster', 'Volume_Based_Volatility']

# If features exist already in HDFS, skip recomputing them to avoid duplication.
missing_features = [c for c in feature_cols if c not in df.columns]
if len(missing_features) == 0:
    logger.info("Found precomputed feature columns in HDFS; skipping feature recomputation")
else:
    logger.info("Missing feature columns (%s). Computing features now.", missing_features)
    # compute features based on Close and Volume
    # ensure Close and Volume exist
    if 'Close' not in df.columns or 'Volume' not in df.columns:
        raise RuntimeError("Input data must contain 'Close' and 'Volume' to compute features")

    # compute Daily_Return and rolling features
    df['Daily_Return'] = df['Close'].pct_change()
    df['Volatility_Cluster'] = df['Daily_Return'].rolling(21).std() * (252**0.5)
    df['Volume_Based_Volatility'] = df['Volume'].rolling(21).std() / df['Volume'].rolling(21).mean()

# create label: risk in next 3 days (>=5% drop)
threshold = -0.05
window = 3
for i in range(1, window+1):
    df[f'{i}d_ret'] = df['Close'].shift(-i) / df['Close'] - 1
df['Future_Min_Return'] = df[[f'{i}d_ret' for i in range(1, window+1)]].min(axis=1)
df['Risk_Event'] = (df['Future_Min_Return'] <= threshold).astype(int)

# Prepare columns for training
cols = feature_cols + ['Risk_Event']
df = df.dropna(subset=cols)

X = df[feature_cols]
y = df['Risk_Event']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

clf = RandomForestClassifier(n_estimators=200, max_depth=6, random_state=42, class_weight='balanced')
clf.fit(X_train, y_train)

# Evaluate on the hold-out test set and print results to terminal
logger.info("Evaluating model on test set (size=%d)", X_test.shape[0])
try:
    y_pred = clf.predict(X_test)
    y_proba = clf.predict_proba(X_test)[:, 1]

    acc = accuracy_score(y_test, y_pred)
    prec = precision_score(y_test, y_pred, zero_division=0)
    rec = recall_score(y_test, y_pred, zero_division=0)
    f1 = f1_score(y_test, y_pred, zero_division=0)
    rocauc = roc_auc_score(y_test, y_proba)
    cm = confusion_matrix(y_test, y_pred)

    logger.info("Test Accuracy: %.4f", acc)
    logger.info("Test Precision: %.4f", prec)
    logger.info("Test Recall: %.4f", rec)
    logger.info("Test F1: %.4f", f1)
    logger.info("Test ROC AUC: %.4f", rocauc)
    logger.info("Confusion Matrix:\n%s", cm)
    logger.info("Classification Report:\n%s", classification_report(y_test, y_pred, zero_division=0))
except Exception as e:
    logger.exception("Failed to evaluate model: %s", e)

joblib.dump(clf, "/app/model.joblib")
logger.info("Model trained and saved to /app/model.joblib")
