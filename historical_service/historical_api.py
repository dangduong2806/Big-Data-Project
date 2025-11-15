from flask import Flask, jsonify, request
from flask_cors import CORS
from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # Thêm import functions
import json

app = Flask(__name__)
CORS(app)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("HDFSDataService") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

HDFS_PATH = "hdfs://namenode:9000/data/raw/stock_data"

@app.route('/historical_data/<ticker>', methods=['GET'])
def get_historical_data(ticker):
    """
    Lấy dữ liệu lịch sử OHLCV từ HDFS cho một ticker cụ thể
    """
    try:
        df = spark.read.parquet(HDFS_PATH)
        
        df_filtered = df.filter(F.upper(df.Ticker) == ticker.upper())
        
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        if start_date:
            df_filtered = df_filtered.filter(df_filtered.timestamp >= start_date)
        if end_date:
            df_filtered = df_filtered.filter(df_filtered.timestamp <= end_date)
            
        limit = int(request.args.get('limit', 365))
        # Lấy các bản ghi MỚI NHẤT
        df_sorted = df_filtered.orderBy('timestamp', ascending=False).limit(limit)
        
        data = df_sorted.toPandas().to_dict('records')
        
        # Sắp xếp lại theo thứ tự TĂNG DẦN (tốt hơn cho biểu đồ)
        data.reverse()

        for record in data:
            if 'timestamp' in record:
                record['timestamp'] = str(record['timestamp'])
        
        return jsonify({
            'ticker': ticker.upper(),
            'count': len(data),
            'data': data
        })
        
    except Exception as e:
        return jsonify({
            'error': str(e),
            'ticker': ticker
        }), 500

@app.route('/available_tickers', methods=['GET'])
def get_available_tickers():
    """
    Lấy danh sách các ticker có sẵn trong HDFS
    """
    try:
        df = spark.read.parquet(HDFS_PATH)
        tickers = [row.Ticker for row in df.select('Ticker').distinct().collect()]
        
        return jsonify({
            'tickers': sorted(tickers)
        })
        
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

@app.route('/ticker_summary/<ticker>', methods=['GET'])
def get_ticker_summary(ticker):
    """
    Lấy thông tin tóm tắt về một ticker
    """
    try:
        df = spark.read.parquet(HDFS_PATH)
        df_ticker = df.filter(F.upper(df.Ticker) == ticker.upper())
        
        summary = df_ticker.agg(
            F.min('Close').alias('min_close'),
            F.max('Close').alias('max_close'),
            F.avg('Close').alias('avg_close'),
            F.min('timestamp').alias('first_date'),
            F.max('timestamp').alias('last_date'),
            F.count('*').alias('total_records'),
            F.avg('Volume').alias('avg_volume')
        ).collect()[0]
        
        # Chuyển đổi an toàn sang float/int, xử lý None
        return jsonify({
            'ticker': ticker.upper(),
            'min_close': float(summary.min_close) if summary.min_close is not None else 0.0,
            'max_close': float(summary.max_close) if summary.max_close is not None else 0.0,
            'avg_close': float(summary.avg_close) if summary.avg_close is not None else 0.0,
            'first_date': str(summary.first_date),
            'last_date': str(summary.last_date),
            'total_records': int(summary.total_records) if summary.total_records is not None else 0,
            'avg_volume': float(summary.avg_volume) if summary.avg_volume is not None else 0.0
        })
        
    except Exception as e:
        return jsonify({
            'error': str(e),
            'ticker': ticker
        }), 500

if __name__ == '__main__':
    # Chạy trên cổng 8001 (đây là cổng nội bộ container)
    app.run(host='0.0.0.0', port=8001, debug=True)