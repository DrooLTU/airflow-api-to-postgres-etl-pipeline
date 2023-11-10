from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

# SET UP DB OPERATOR
# CREATE TABLES


# TWO OF THESE SO MUST BE MODULAR:
# NEEDS A DOCKER OPERATOR TO RUN PYTHON SCRIPT IN CONTAINER

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
       volumes=["/tmp/airflow/data:/data"],
   )

# TRANSFORM DATA TO STAR SCHEMA

# WRITE DATA TO DB