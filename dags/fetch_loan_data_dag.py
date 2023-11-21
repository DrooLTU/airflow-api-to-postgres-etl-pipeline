from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

# SET UP DB OPERATOR
# CREATE TABLES
default_args = {
    "owner": "airflow",
    "start_date": datetime.now(),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# TWO OF THESE SO MUST BE MODULAR:
# NEEDS A DOCKER OPERATOR TO RUN PYTHON SCRIPT IN CONTAINER


# TRANSFORM DATA TO STAR SCHEMA

# WRITE DATA TO DB

with DAG(
    "fetch_loan_data_dag",
    default_args=default_args,
    description="DAG to create loans database in PostgreSQL",
    schedule=timedelta(days=1),
) as dag:
    fetch_data = DockerOperator(
        task_id="fetch_data",
        image="justinaslorjus/kaggle_fetch_dataset:1.0-3.11",
        command=[
            'fetch-data',
            '--dataset_name',
            'vikasukani/loan-eligible-dataset',
            '--output_path',
            '/data',
        ],
        mount_tmp_dir=False,
        mounts=[
            Mount(source='kaggle-data-volume', target="/data", type="volume"),
        ]
    )

if __name__ == "__main__":
    dag.cli()
