from fastapi import FastAPI
import joblib
import pandas as pd
import numpy as np

app = FastAPI(title="Fraud Detection API")

# Load model khi khởi động container
MODEL_PATH = "/app/model/fraud_xgb_pipeline.pkl"
clf = joblib.load(MODEL_PATH)

@app.post("/predict")
def predict(transaction: dict):
    """
    Dự đoán 1 giao dịch.
    Input: JSON {"TransactionAmt": 100.0, "ProductCD": "W", ...}
    """
    X = pd.DataFrame([transaction])
    y_pred = clf.predict(X)[0]
    y_prob = clf.predict_proba(X)[0][1]
    return {"isFraud": int(y_pred), "fraud_probability": float(y_prob)}
