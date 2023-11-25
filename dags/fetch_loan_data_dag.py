import os
import glob
from dotenv import load_dotenv

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.hooks.postgres_hook import PostgresHook

from docker.types import Mount

import pandas as pd

from sqlalchemy import create_engine

from datetime import datetime, timedelta

load_dotenv()

PROJECT_NAME = os.getenv("COMPOSE_PROJECT_NAME")
VOLUME_NAME = f"{PROJECT_NAME}_kaggle-data-volume"


KAGGLE_API_KEY = Variable.get("kaggle_api_key", default_var=None)
KAGGLE_API_USERNAME = Variable.get("kaggle_api_username", default_var=None)

LOAN_DB_NAME = 'loans'


default_args = {
    "owner": "airflow",
    "start_date": datetime.now(),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


def _check_postgres_db_exists(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id='MainPG')
    conn = postgres_hook.get_conn()

    query = f"SELECT 1 FROM pg_database WHERE datname='{LOAN_DB_NAME}';"

    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
    db_exists = result is not None
    kwargs['ti'].xcom_push(key='check_db_result', value=db_exists)
    
    return db_exists


def _transform_data(root_path: str = "/opt/airflow/data", output_path: str = "/opt/airflow/data", **kwargs):
    """
    Transfrom loan data into star schema.

    Args:
    root_path: Path to the folder to be compressed.
    output_path: Path where to store the result.
    """
    if not os.path.exists(root_path):
        raise AirflowException(f"The folder {root_path} does not exist.")

    if not os.path.exists(output_path):
        print(f"Creating the folder {output_path}.")
        os.makedirs(output_path)

    folder_path = os.path.join(root_path, "*.csv")
    csv_files = glob.glob(folder_path)

    postgres_conn = BaseHook.get_connection('LoansDB')

    connection_str = f"postgresql://{postgres_conn.login}:{postgres_conn.password}@{postgres_conn.host}:{postgres_conn.port}/{postgres_conn.schema}"
    engine = create_engine(connection_str)

    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        table_name_root = os.path.splitext(os.path.basename(csv_file))[0]

        df_name = f'{table_name_root}_fact_table'
        fact_table_df = df[['Loan_ID', 'LoanAmount', 'Loan_Amount_Term', 'Credit_History']]
        fact_table_df.set_index('Loan_ID', inplace=True)
        fact_table_df.to_sql(df_name, engine, if_exists='replace', index=True)
        fact_table_df.to_csv(f'{output_path}/{df_name}.csv', index=True)

        df_name = f'{table_name_root}_borrower_dimension'
        borrower_df = df[['Loan_ID', 'Gender', 'Married', 'Dependents', 'Education', 'Self_Employed']]
        borrower_df.to_sql(f'{table_name_root}_borrower_dimension', engine, if_exists='replace', index=False)
        borrower_df.to_csv(f'{output_path}/{df_name}.csv', index=True)

        df_name = f'{table_name_root}_income_dimension'
        income_df = df[['Loan_ID', 'ApplicantIncome', 'CoapplicantIncome']]
        income_df.to_sql(f'{table_name_root}_income_dimension', engine, if_exists='replace', index=False)
        income_df.to_csv(f'{output_path}/{df_name}.csv', index=True)

        df_name = f'{table_name_root}_location_dimension'
        location_df = df[['Loan_ID', 'Property_Area']]
        location_df.to_sql(f'{table_name_root}_location_dimension', engine, if_exists='replace', index=False)
        location_df.to_csv(f'{output_path}/{df_name}.csv', index=True)

    print("Data written to database")


with DAG(
    "fetch_loan_data_dag",
    default_args=default_args,
    description="DAG to create loans database in PostgreSQL",
    schedule=timedelta(days=1),
) as dag:
    
    check_db_task = PythonOperator(
        task_id='check_db_task',
        python_callable=_check_postgres_db_exists,
        provide_context=True,
        dag=dag,
    )
    
    create_loans_db_task = PostgresOperator(
        task_id='create_loans_db_task',
        sql=f"CREATE DATABASE {LOAN_DB_NAME};",
        postgres_conn_id='MainPG',
        autocommit=True,
    )

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=lambda **kwargs: 'create_loans_db_task' if kwargs['ti'].xcom_pull(key='check_db_result') is False else 'fetch_data_task',
        provide_context=True,
        dag=dag,
    )

    fetch_data_task = DockerOperator(
        task_id="fetch_data_task",
        image="justinaslorjus/kaggle_fetch_dataset:1.0-3.11",
        trigger_rule="none_failed",
        command=[
            "/bin/bash",
            "-c",
            f"export KAGGLE_USERNAME={KAGGLE_API_USERNAME} && "
            f"export KAGGLE_KEY={KAGGLE_API_KEY} && "
            "fetch-data --dataset_name vikasukani/loan-eligible-dataset --output_path /data",
        ],
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=[
            Mount(source=VOLUME_NAME, target="/data", type="volume"),
        ],
    )

    transform_data_task = PythonOperator(
        task_id="transform_data_task",
        python_callable=_transform_data,
        provide_context=True,
        op_kwargs={"output_path": "/opt/airflow/data/transformed"},
    )

    check_db_task >> branch_task
    branch_task >> [create_loans_db_task , fetch_data_task]
    create_loans_db_task >> fetch_data_task >> transform_data_task
    fetch_data_task >> transform_data_task
    

if __name__ == "__main__":
    dag.cli()
