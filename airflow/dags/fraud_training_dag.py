from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

@dag(
    dag_id="fraud_model_training",
    start_date=datetime(2025, 1, 1),
    schedule="@weekly",
    catchup=False
)
def fraud_training_pipeline():
    
    task_train_fraud = DockerOperator(
        task_id="train_fraud_model",
        image="fraud-train", # 50:50
        command="python train_fraud_model.py",
        # network_mode="your_project_name_default", # THAY TÊN NÀY
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        # Mount volume để model được lưu ra ngoài
        mounts=["./model:/app/model"] # THAY ĐƯỜNG DẪN NÀY
    )

# Gọi DAG
fraud_training_pipeline()