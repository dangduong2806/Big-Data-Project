from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator # <--- Thêm cái này
from airflow.utils.trigger_rule import TriggerRule # <--- Quan trọng để hồi phục service
from datetime import datetime

@dag(
    dag_id="deploy_pipeline",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1, # Thêm phần này
    schedule_interval=None # Set schedule_interval = None để airflow ko tự động chạy thêm phiên thứ 2 chiếm tài nguyên của spark
)
def deploy_pipeline():
    
    # Kafka-producer
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

    # 4. Task bật Model Service
    start_model_service_market_risk = BashOperator(
        task_id="start_model_services",
        bash_command="docker start model-service-gspc model-service-xom model-service-bp model-service-cvx",
    )

    #Chạy task fraud detection
    start_fraud_app = BashOperator(
        task_id="start_fraud_app",
        bash_command="docker start fraud-app"
    )
    start_fraud_producer = BashOperator(
        task_id="start_fraud_producer",
        bash_command="docker start fraud-producer"
    )
    start_fraud_inference = BashOperator(
        task_id="start_fraud_detection_inference",
        bash_command="docker start fraud-consumer"
    )

    # 1.1. Delay seconds để đợi kafka_producer đọc dữ liệu xong
    delayy = BashOperator(
        task_id="delay_after_stop",
        bash_command="sleep 7"
    )

    # --- ĐỊNH NGHĨA LUỒNG CHẠY ---
    run_kafka_producer
    delayy >> start_model_service_market_risk

    start_fraud_app
    delayy >> start_fraud_inference
    start_fraud_producer

# Gọi hàm
deploy_pipeline()