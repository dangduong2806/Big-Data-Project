import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.metrics import classification_report
import xgboost as xgb
import joblib
import os

# Đường dẫn
DATA_PATH = os.getenv("DATA_PATH", "/app/data/train_transaction.csv")
MODEL_PATH = os.getenv("MODEL_PATH", "/app/model/fraud_xgb_pipeline.pkl")

print(f"Đọc dữ liệu từ: ", DATA_PATH)
df = pd.read_csv(DATA_PATH)

# Tách nhãn
X = df.drop(columns=["isFraud"])
y = df["isFraud"]

# Xác định loại cột
numeric_features = X.select_dtypes(include=['int64', 'float64']).columns
categorical_features = X.select_dtypes(include=['object']).columns

print(f"Numeric features: {len(numeric_features)}")
print(f"Categorical features: {len(categorical_features)}")

# Pipeline tiền xử lý
numeric_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='median')),
    ('scaler', StandardScaler())
])

categorical_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
    ('onehot', OneHotEncoder(handle_unknown='ignore', sparse_output=False))
])

preprocessor = ColumnTransformer(transformers=[
    ('num', numeric_transformer, numeric_features),
    ('cat', categorical_transformer, categorical_features)
])

# Chia dữ liệu
from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, stratify=y, random_state=42
)

# Khởi tạo model
model = xgb.XGBClassifier(
    n_estimators=300,
    learning_rate=0.1,
    max_depth=6,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42,
    n_jobs=-1,
    eval_metric='auc'
)

# Pipeline hoàn chỉnh
clf = Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('classifier', model)
])

# ---- Huấn luyện
print("Bắt đầu huấn luyện...")
clf.fit(X_train, y_train)
print("✅ Huấn luyện xong.")

# ---- Đánh giá
y_pred = clf.predict(X_test)
print("Classification Report:")
print(classification_report(y_test, y_pred))

# ---- Lưu model
os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
joblib.dump(clf, MODEL_PATH)
print(f"💾 Đã lưu model vào {MODEL_PATH}")