from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, date, timedelta
import pandas as pd
import sys
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

sys.path.append('/opt/airflow/dags/scripts')
from ingestion import ingestion, preparation
from config import configs

config_file = configs

def _t0(ti):
    filename = ingestion()
    ti.xcom_push(key='filename', value=filename)

def _t1(ti):
    values, cols = preparation(ti.xcom_pull(key='filename', task_ids='t0'))
    sql = f"INSERT INTO cadastro {cols} VALUES {values};"
    ti.xcom_push(key='sql', value=sql)

def _t2(ti):
    sql = ti.xcom_pull(key='sql', task_ids='t1')
    mysql_hook = MySqlHook(mysql_conn_id='mysql_con', schema = 'db')
    mysql_hook.run(sql)
    

with DAG(
    "de04-dataops",
    start_date=datetime(2023, 10, 10), 
    schedule_interval=timedelta(minutes=2),
    catchup=False) as dag:

    t0 = PythonOperator(
        task_id='t0',
        python_callable=_t0
    )

    t1 = PythonOperator(
        task_id='t1',
        python_callable=_t1
    )

    t2 = PythonOperator(
        task_id='t2',
        python_callable=_t2
    )

    t0 >> t1 >> t2