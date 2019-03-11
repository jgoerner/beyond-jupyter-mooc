from datetime import datetime, timedelta
import logging
import os
import time

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
def log_bye():
    logging.info("Bye Bye")

def do_sleep(t):
    time.sleep(t)

def log_now():
    logging.info(datetime.now())

t1 = PythonOperator(
    task_id="initial-time",
    python_callable=log_now,
    dag=dag,
)

t2 = PythonOperator(
    task_id="sleep",
    python_callable=do_sleep,
    op_kwargs={"t": 2},
    dag=dag,
)

t3 = PythonOperator(
    task_id="time-after-sleep",
    python_callable=log_now,
    dag=dag,
)

t4 = PythonOperator(
    task_id="bye-greetings",
    python_callable=log_bye,
    dag=dag,
)

# ORDERING
t1 >> t2 >> t3 >> t4
