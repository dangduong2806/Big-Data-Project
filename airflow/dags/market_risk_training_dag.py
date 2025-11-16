from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

# Dùng DockerOperator để chạy các container mà bạn đã định nghĩa
# Chúng sẽ chạy trên cùng một mạng "default" của docker-compose

@dag(
    dag_id="market_risk_training",
    start_date=datetime(2025, 1, 1),
    schedule="@weekly", # Chạy hàng tuần
    catchup=False
)
def market_risk_training_pipeline():
    
    # Task 1: Chạy save_to_hdfs_job.py
    # Chú ý: Image phải là tên được build từ `docker-compose build spark-jobs`
    # Thường nó sẽ là <tên_thư_mục>-spark-jobs
    task_save_to_hdfs = DockerOperator(
        task_id="save_to_hdfs",
        image="spark-jobs", # 50:50
        command="python /app/save_to_hdfs_job.py",
        # network_mode="your_project_name_default", # THAY TÊN NÀY
        auto_remove=True,
        docker_url="unix://var/run/docker.sock"
    )

    # Task 2: Chạy train_job.py
    task_train_model = DockerOperator(
        task_id="train_risk_model",
        image="train-job", # 50:50
        command="python /app/train_job.py",
        # network_mode="your_project_name_default",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock"
    )

    # Định nghĩa thứ tự chạy
    task_save_to_hdfs >> task_train_model

# Gọi DAG
market_risk_training_pipeline()