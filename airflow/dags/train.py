from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator # <--- Thêm cái này
from datetime import datetime

from docker.types import Mount

@dag(
    dag_id="training_pipeline",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1, # Thêm phần này
    schedule_interval=None # Set schedule_interval = None để airflow ko tự động chạy thêm phiên thứ 2 chiếm tài nguyên của spark
)
def train_pipeline():

    # 0 Task kafka-producer luôn chạy song song
    # run_kafka_producer = DockerOperator(
    #     task_id = "kafka_producer",
    #     image = "kafka-producer:latest",
    #     command = "python /app/producer.py",
    #     docker_url="unix://var/run/docker.sock",
    #     network_mode = "btl_scale_default"
    # )
    run_kafka_producer = BashOperator(
        task_id = "kafka_producer",
        bash_command = "docker start kafka-producer"
    )
    
    # 1. Task tắt Model Service để giải phóng Spark
    stop_model_service = BashOperator(
        task_id="stop_model_service",
        bash_command="docker stop model-service-gspc model-service-xom model-service-bp model-service-cvx" 
        # Lưu ý: container này phải có tên đúng như trong docker-compose
    )

    # 2. Task chính: Save to HDFS
    task_save_to_hdfs = DockerOperator(
        task_id="save_to_hdfs",
        image="spark-jobs:latest", 
        command="python3 /app/save_to_hdfs_job.py",
        auto_remove=False,
        docker_url="unix://var/run/docker.sock",
        network_mode="big-data-project_default", # chạy docker network ls để lấy tên mạng của docker-compose.yml trên máy
        mount_tmp_dir=False
    )

    # 3. Task Train (chạy sau khi save)
    task_train_risk = DockerOperator(
        task_id="train_risk_models",
        image="train-job:latest",
        command="python3 /app/train_job.py",
        auto_remove=False,
        docker_url="unix://var/run/docker.sock",
        network_mode="big-data-project_default", # chạy docker network ls để lấy tên mạng của docker-compose.yml trên máy
        mount_tmp_dir=False
    )

    # (training fraud)
    task_train_fraud = DockerOperator(
        task_id="train_fraud_model",
        image="fraud-train:latest", # Thêm tên image
        # Sửa phần này
        command="python3 /app/train_fraud_model.py",
        network_mode="big-data-project_default", # chạy docker network ls để lấy tên mạng của docker-compose.yml trên máy
        auto_remove=False,
        docker_url="unix://var/run/docker.sock",
        mounts=[
            Mount(source='E:\\bigData\Big-Data-Project\data', target='/app/data', type='bind') # Thay bằng đường dẫn trên local
        ]
    )

    # Chạy đọc dữ liệu test cho fraud-detection và topic praud-predictions luôn
    start_fraud_producer = BashOperator(
        task_id="start_fraud_producer",
        bash_command="docker start fraud-producer"
    )

    # 1.1. Delay seconds để đợi kafka_producer đọc dữ liệu xong
    delayy = BashOperator(
        task_id="delay_after_stop",
        bash_command="sleep 7"
    )

    # --- ĐỊNH NGHĨA LUỒNG CHẠY ---
    run_kafka_producer
    start_fraud_producer
    task_train_fraud
    # Tắt service -> Chạy Job Spark -> Train -> Bật lại service
    stop_model_service >> delayy >> task_save_to_hdfs >> task_train_risk
# Gọi hàm
train_pipeline()