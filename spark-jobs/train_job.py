import os
import logging
from pyspark.sql import SparkSession

# Thêm phần này
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

import math
from functools import reduce

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("train_job")

# Configuration: allow overriding HDFS path via env var. Try processed path first,
# then fallback to raw path where the streaming job writes features.
TRAIN_PARQUET_PATH = os.environ.get(
    "TRAIN_PARQUET_PATH",
    "hdfs://namenode:9000/data/raw/stock_data"
)
FALLBACK_PARQUET_PATH = "hdfs://namenode:9000/data/raw/stock_data"

spark = SparkSession.builder \
    .appName("train-risk-model") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
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

# Không convert toàn bộ sang pandas luôn mà xử lý trên pyspark dataframe, 
# rồi mới sample sang pandas để huấn luyênj

df_spark = read_parquet_with_fallback(TRAIN_PARQUET_PATH, FALLBACK_PARQUET_PATH)
# Kiểm tra cột Company có trong df_spark không
if 'Company' not in df_spark.columns:
    raise RuntimeError("Missing 'Company' column in dataset. Each record must belong to a company.")

companies = df_spark.select("Company").distinct().rdd.flatMap(lambda x: x).collect()
logger.info(f"Found {len(companies)} companies: {companies}")

for company in companies:
    logger.info(f"====================== Training model for {company} ======================")

    df_company = df_spark.filter(F.col("Company") == company)

    count_before = df_company.count()
    logger.info(f"{company}: raw data count = {count_before}")

    # Normalize time column: accept 'Date' or 'timestamp'
    if 'Date' in df_company.columns:
        time_col = 'Date'
    elif 'timestamp' in df_company.columns:
        time_col = 'timestamp'
    else:
        # fallback to first column if it looks like a datetime index
        time_col = df_company.columns[0]

    # ensure datetime and sort
    try:
        # df_company[time_col] = pd.to_datetime(df_company[time_col])
        # df_company = df_company.sort_values(time_col)
        windowSpec = Window.orderBy(time_col)
    except Exception:
        logger.warning("Could not parse/convert time column %s; proceeding without sorting", time_col)

    # Feature columns that streaming producer already writes
    feature_cols = ['Daily_Return', 'Volatility_Cluster', 'Volume_Based_Volatility']

    # If features exist already in HDFS, skip recomputing them to avoid duplication.
    
    # compute features based on Close and Volume
    # ensure Close and Volume exist
    if 'Close' not in df_company.columns or 'Volume' not in df_company.columns:
        raise RuntimeError("Input data must contain 'Close' and 'Volume' to compute features")

    # compute Daily_Return and rolling features
    # --- Daily_Return ---
    df_company = df_company.withColumn("Daily_Return",
                                    (F.col("Close") - F.lag("Close").over(windowSpec)) / F.lag("Close").over(windowSpec))
    # --- Volatility_Cluster (rolling 21 ngày std) ---
    rolling_window = Window.orderBy("timestamp").rowsBetween(-20, 0)  # 21 rows including current
    df_company = df_company.withColumn(
        "Volatility_Cluster",
        F.stddev("Daily_Return").over(rolling_window) * math.sqrt(252)
    )
    # --- Volume_Based_Volatility ---
    df_company = df_company.withColumn("Volume_Based_Volatility",
                                    F.stddev("Volume").over(rolling_window) / F.mean("Volume").over(rolling_window))
        
    # create label: risk in next 3 days (>=5% drop)
    # window_future = Window.orderBy("timestamp").rowsBetween(1, 5)  # 5 ngày tương lai
    # df_company = df_company.withColumn("Future_Min_Return",
    #                                    F.min((F.col("Close") - F.lag("Close", -1).over(windowSpec)) / F.col("Close")).over(window_future))
    
    # df_company = df_company.withColumn("Risk_Event", F.when(F.col("Future_Min_Return") <= -0.03, 1).otherwise(0))
    future_returns = [F.lead("Close", i).over(windowSpec) / F.col("Close") - 1 for i in range(1,3)]
    df_company = df_company.withColumn("Future_Min_Return", reduce(lambda a,b: F.least(a,b), future_returns))
    df_company = df_company.withColumn("Risk_Event", F.when(F.col("Future_Min_Return") <= -0.02, 1).otherwise(0))
    
    # Prepare columns for training
    # Chỉ xóa NaN các cột quan trọng
    df_company = df_company.select(feature_cols + ["Risk_Event"]).dropna(subset=feature_cols)
    # Thay bằng giá trị 0
    df_company = df_company.fillna({"Daily_Return": 0, "Volatility_Cluster":0, "Volume_Based_Volatility": 0})

    if df_company.count() == 0:
        logger.info(f"{company} dữ liệu sau khi tính toán đặc trưng bị NaN hết")
    else:
        logger.info(f"Dữ liệu {company} chuẩn bị để train")
    
    # Không Convert sang Pandas để train, dùng Pyspark MLlib
    # để train và lưu model trên hdfs 
    # --- Assemble features ---
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="Features")
    df_vector = assembler.transform(df_company).select("Features", "Risk_Event")
    train_df, test_df = df_vector.randomSplit([0.6, 0.4], seed=42)

    if train_df.count() == 0:
        logger.info("Dataset trống, không thể train model.")
        raise ValueError("Dataset trống, không thể train model.")
    # Train model
    try:
        logger.info("Training Random Forest model...")
        # --- Model: RandomForest (Spark MLlib) ---
        rf = RandomForestClassifier(
            labelCol="Risk_Event",
            featuresCol="Features",
            numTrees=200,
            maxDepth=6,
            seed=42
        )
        # --- Pipeline (có thể mở rộng sau này) ---
        pipeline = Pipeline(stages=[rf])
        model = pipeline.fit(train_df)
        # Evaluate on the hold-out test set
        logger.info("Evaluating model on test set")
        preds = model.transform(test_df)

        # Tính các chỉ số metrics
        binary_eval = BinaryClassificationEvaluator(
            labelCol="Risk_Event",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )
        auc = binary_eval.evaluate(preds)
        # Multiclass evaluator (cho accuracy, precision, recall, f1)
        multi_eval = MulticlassClassificationEvaluator(
            labelCol="Risk_Event",
            predictionCol="prediction"
        )
        accuracy = multi_eval.setMetricName("accuracy").evaluate(preds)
        precision = multi_eval.setMetricName("weightedPrecision").evaluate(preds)
        recall = multi_eval.setMetricName("weightedRecall").evaluate(preds)
        f1 = multi_eval.setMetricName("f1").evaluate(preds)

        logger.info(f"=== Evaluation Metrics for {company} ===")
        logger.info(f"AUC: {auc:.4f}")
        logger.info(f"Accuracy: {accuracy:.4f}")
        logger.info(f"Precision: {precision:.4f}")
        logger.info(f"Recall: {recall:.4f}")
        logger.info(f"F1 Score: {f1:.4f}")

        # Save model to HDFS
        # path = f"/models/{company}_risk_model.joblib"
        # model_path = os.environ.get('MODEL_SAVE_PATH', path)
        # os.makedirs(os.path.dirname(model_path), exist_ok=True)
        model_path = f"hdfs://namenode:9000/models/{company}_risk_model"
        model.write().overwrite().save(model_path)
        # joblib.dump(model, model_path)
        logger.info("Model training and saving completed successfully")

    except Exception as e:
        logger.exception("Failed during model training or saving: %s", e)
        raise
