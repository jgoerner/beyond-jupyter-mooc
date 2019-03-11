from datetime import datetime, timedelta
import logging
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

### DAG ###
default_args={
        "owner":"airflow",
        "depends_on_past":"false",
        "start_date":datetime.today()-timedelta(days=1),
        }

dag = DAG(
    dag_id="testing",
    default_args=default_args,
    schedule_interval=timedelta(seconds=30))

### TASK ###
def hello_world():
    logging.info("Hello World from the Task")

task = PythonOperator(
    task_id="testing-task",
    python_callable=hello_world,
    dag=dag,
)
