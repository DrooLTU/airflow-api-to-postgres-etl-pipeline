from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

# SET UP DB OPERATOR
# CREATE TABLES
default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
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
    schedule_interval=timedelta(days=1),
) as dag:
    fetch_data = DockerOperator(
        task_id="fetch_data",
        image="my-docker-image/name-here",
        command=[
            "fetch-data",
            "--input_path",
            "/data/input/loans/{{ds}}.json",
            "--output_path",
            "/data/output/loans/{{ds}}.csv",
        ],
        api_version="auto",
        auto_remove=True,
        volumes=["/tmp/airflow/data:/data"],
        network_mode="bridge",
    )

if __name__ == "__main__":
    dag.cli()
