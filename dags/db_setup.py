from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'create_loans_db_dag',
    default_args=default_args,
    description='DAG to create loans database in PostgreSQL',
    schedule_interval=timedelta(days=1),
) as dag:

    create_loans_db_task = PostgresOperator(
        task_id='create_loans_db_task',
        sql="CREATE DATABASE IF NOT EXISTS loans;",
        postgres_conn_id='MainPG',
        autocommit=True,
    )

if __name__ == "__main__":
    dag.cli()