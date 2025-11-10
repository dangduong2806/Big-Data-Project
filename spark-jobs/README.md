File save_to_hdfs.py để nhận dữ liệu truyền từ Kafka (đọc realtime từ yahoo finance api) đến và lưu vào HDFS phục vụ cho việc train model.

File train_job.py để lấy dữ liệu trên HDFS (là kết quả sau khi chạy file save_to_hdfs.py) để train model và test model (kết quả test của model thì đang để hiện lên terminal).