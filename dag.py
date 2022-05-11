from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import datetime as dt
from airflow import DAG
import papermill as pm
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    'owner': 'datawarehouse',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
} 

def end():
    print('finished')

with DAG(
    dag_id='datawarehouse',
    default_args=default_args,
    schedule_interval="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False
) as dag_1:
    task0=DummyOperator(task_id="start")
    task3=DummyOperator(task_id="finish")
    task1 = BashOperator(
        task_id='Processing',
        bash_command=f'jupyter nbconvert --execute --clear-output  /c/Users/yarac/Desktop/datawarehouse/dags/Process2.ipynb',
    )
    task2=PythonOperator(
        task_id="end",
        python_callable= end)

task0 >> task1 >> task2 >> task3
