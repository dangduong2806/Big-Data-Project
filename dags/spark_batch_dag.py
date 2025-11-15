# dags/spark_batch_dag.py
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from docker.types import Mount

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Lấy tên network của Docker Compose (thường là <tên_thư_mục>_default)
# Bạn cần kiểm tra tên network bằng lệnh `docker network ls`
# Giả sử tên network là 'big-data-project_default'
DOCKER_NETWORK = 'big-data-project_default'

with DAG(
    'spark_batch_pipeline',
    default_args=default_args,
    description='DAG để chạy job HDFS và Train model',
    schedule_interval='@daily', # Chạy hàng ngày
    start_date=days_ago(1),
    catchup=False,
    tags=['spark', 'batch'],
) as dag:

    # Task 1: Chạy save_to_hdfs_job.py
    save_to_hdfs = DockerOperator(
        task_id='save_to_hdfs',
        image='spark-jobs-image:latest',# Dùng cùng image với spark-jobs cũ
        force_pull=False,
        command="""
            /opt/spark/bin/spark-submit
            --master spark://spark-master:7077
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-client:3.3.4
            /app/save_to_hdfs_job.py
        """,
        network_mode=DOCKER_NETWORK,
        mounts=[
            Mount(source='big-data-project_hadoop_conf', target='/opt/hadoop/conf', type='volume'),
            Mount(source='C:/Users/Trong Duc/Big-Data-Project/spark-jobs', target='/app', type='bind')
        ],
        environment={
            'SPARK_HADOOP_FS_DEFAULTFS': 'hdfs://namenode:9000',
            'HADOOP_CONF_DIR': '/opt/hadoop/conf',
        },
        user='root',
        auto_remove=True
    )

    # Task 2: Chạy train_job.py
    train_model = DockerOperator(
        task_id='train_model',
        image='spark-jobs-image:latest', # Dùng cùng image với train-job cũ
        force_pull=False,
        command="""
            /opt/spark/bin/spark-submit
            --master spark://spark-master:7077
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-client:3.3.4
            /app/train_job.py
        """,
        network_mode=DOCKER_NETWORK,
        mounts=[
            Mount(source='big-data-project_hadoop_conf', target='/opt/hadoop/conf', type='volume'),
            Mount(source='C:/Users/Trong Duc/Big-Data-Project/spark-jobs', target='/app', type='bind'),
            Mount(source='C:/Users/Trong Duc/Big-Data-Project/model_service', target='/models', type='bind')
        ],
        environment={
            'SPARK_HADOOP_FS_DEFAULTFS': 'hdfs://namenode:9000',
            'HADOOP_CONF_DIR': '/opt/hadoop/conf',
            'MODEL_SAVE_PATH': '/models/risk_model.joblib',
        },
        user='root', # Giống trong docker-compose cũ
        auto_remove=True
    )

    # Định nghĩa thứ tự chạy
    save_to_hdfs >> train_model