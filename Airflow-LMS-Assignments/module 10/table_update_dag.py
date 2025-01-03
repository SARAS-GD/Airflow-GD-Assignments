from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='table_update_dag',
    default_args=default_args,
    description='A simple DAG for table updates',
    schedule_interval=None,
    start_date=datetime(2024, 6, 1),
    catchup=False,
) as dag:


    start_task = EmptyOperator(task_id='start')



    start_task 
