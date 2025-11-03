from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

def create_spark_session():
    """Tạo Spark Session với các cấu hình cần thiết"""
    return SparkSession.builder \
        .appName("save-to-hdfs") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5") \
        .getOrCreate()

def create_schema():
    """Định nghĩa schema cho dữ liệu từ Kafka"""
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("Open", DoubleType(), True),
        StructField("High", DoubleType(), True),
        StructField("Low", DoubleType(), True),
        StructField("Close", DoubleType(), True),
        StructField("Volume", LongType(), True),
        StructField("Daily_Return", DoubleType(), True),
        StructField("Volatility_Cluster", DoubleType(), True),
        StructField("Volume_Based_Volatility", DoubleType(), True)
    ])

def process_batch(df, epoch_id):
    """Xử lý và lưu từng batch dữ liệu vào HDFS"""
    # Chuyển đổi timestamp string sang timestamp
    df = df.withColumn("timestamp", to_timestamp("timestamp"))
    
    # Sắp xếp theo timestamp để đảm bảo tính nhất quán
    df = df.orderBy("timestamp")
    
    # Lưu vào HDFS dạng parquet với partition theo ngày
    df.write \
        .partitionBy("year", "month", "day") \
        .mode("append") \
        .parquet("hdfs://namenode:9000/data/raw/stock_data")

def main():
    # Tạo Spark Session
    spark = create_spark_session()
    
    # Đọc dữ liệu từ Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "raw_prices") \
        .load()
    
    # Parse JSON và áp dụng schema
    schema = create_schema()
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Thêm các cột year, month, day để partition
    processed_df = parsed_df \
        .withColumn("timestamp", to_timestamp("timestamp")) \
        .withColumn("year", year("timestamp")) \
        .withColumn("month", month("timestamp")) \
        .withColumn("day", dayofmonth("timestamp"))
    
    # Bắt đầu streaming query
    query = processed_df \
        .writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .start()
    
    # Chờ đến khi job bị dừng
    query.awaitTermination()

if __name__ == "__main__":
    main()