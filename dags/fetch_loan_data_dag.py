import os
import glob
from dotenv import load_dotenv

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

from docker.types import Mount

import pandas as pd

from datetime import datetime, timedelta

# SET UP DB OPERATOR
# CREATE TABLES
# TWO OF THESE SO MUST BE MODULAR:
# NEEDS A DOCKER OPERATOR TO RUN PYTHON SCRIPT IN CONTAINER
# TRANSFORM DATA TO STAR SCHEMA
# WRITE DATA TO DB

load_dotenv()

PROJECT_NAME = os.getenv("COMPOSE_PROJECT_NAME")
VOLUME_NAME = f"{PROJECT_NAME}_kaggle-data-volume"


default_args = {
    "owner": "airflow",
    "start_date": datetime.now(),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


def _transform_data(root_path:str = '/opt/airflow/data', output_path:str = '/opt/airflow/data'):
    """
    Transfrom loan data into star schema.

    Args:
    root_path: Path to the folder to be compressed.
    output_path: Path where to store the result.
    """
    if not os.path.exists(root_path):
        raise AirflowException(f"The folder {root_path} does not exist.")
    
    if not os.path.exists(output_path):
        print(f'Creating the folder {output_path}.')
        os.makedirs(output_path)
    dfs = []
    folder_path = os.path.join(root_path, '*.csv')
    csv_files = glob.glob(folder_path)

    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        dfs.append(df)


    combined_df = pd.concat(dfs, ignore_index=True)

    print(combined_df)



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
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=[
            Mount(source=VOLUME_NAME, target="/data", type="volume"),
        ]
    )

    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=_transform_data,
        op_kwargs={"output_path": "/opt/airflow/data/transformed"},
    )

    fetch_data >> transform_data

if __name__ == "__main__":
    dag.cli()
