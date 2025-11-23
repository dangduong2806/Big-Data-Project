from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import os
import logging

def create_spark_session():
    """Tạo Spark Session với các cấu hình cần thiết"""
    # Thêm 3 dòng cuối
    return SparkSession.builder \
        .appName("save-to-hdfs") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.executor.instances", "1") \
        .config("spark.executor.cores", "4") \
        .config("spark.executor.memory", "4g") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .getOrCreate()

def create_schema():
    """Định nghĩa schema cho dữ liệu từ Kafka"""
    return StructType([
        # Thêm tin công ty
        StructField("Company", StringType(), True),
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

import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

companies = ["GSPC", "XOM", "CVX", "BP"]

def main(company : str, spark):
    try:
        logging.info("Starting Spark Session creation...")
        
        if spark is not None:
            logging.info("Spark Session created successfully")
        
        logging.info("Reading data from Kafka topic 'historical_prices'...")
        historical_topic = "historical_prices_" + company
        # Đọc dữ liệu từ Kafka (sử dụng batch read thay vì streaming)
        df = spark \
            .read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", historical_topic) \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()
        
        # Kiểm tra số lượng records từ Kafka
        kafka_count = df.count()
        logging.info(f"Read {kafka_count} records from Kafka")
        
        if kafka_count == 0:
            logging.error("No data found in Kafka topic")
            return
            
        logging.info("Parsing JSON data with schema...")
        # Parse JSON và áp dụng schema
        schema = create_schema()
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Kiểm tra số lượng records sau khi parse
        parsed_count = parsed_df.count()
        logging.info(f"Parsed {parsed_count} records successfully")
        
        logging.info("Processing timestamps and creating partitions...")
        # Thêm các cột year, month, day để partition
        processed_df = parsed_df \
            .withColumn("timestamp", to_timestamp("timestamp")) \
            .withColumn("year", year("timestamp")) \
            .withColumn("month", month("timestamp")) \
            .withColumn("day", dayofmonth("timestamp"))
        
        # Kiểm tra dữ liệu đã xử lý
        processed_count = processed_df.count()
        logging.info(f"Processed {processed_count} records with partitioning columns")
        
        # Show một số sample dữ liệu
        logging.info("Sample of processed data:")
        processed_df.show(5, truncate=False)
        
        logging.info("Writing data to HDFS...")
        # Lưu trực tiếp vào HDFS (không cần streaming)
        # Sửa mode từ overwrite sang append.
        processed_df.write \
            .partitionBy("Company","year", "month", "day") \
            .mode("append") \
            .parquet("hdfs://namenode:9000/data/raw/stock_data")
            
        logging.info("Data successfully written to HDFS")
        
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}", exc_info=True)

if __name__ == "__main__":
    spark = create_spark_session()
    for company in companies:
        main(company, spark)