Ae chạy docker-compose up -d --build.  
Vào container kafka producer để chạy file producer.py -> Kafka đọc dữ liệu từ yahoo finance api.  
Vào container spark jobs để chạy file save_to_hdfs_job.py -> Đẩy dữ liệu ở Kafka lên HDFS.  
Vào container spark jobs để chạy file train_job.py -> Huấn luyện mô hình.  
Vào container model service chạy file serve.py -> Chạy model inference real-time.
