from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='trigger_table_update_dag',
    default_args=default_args,
    description='A DAG that waits for a file, triggers an external DAG, and removes the file',
    schedule_interval=None, 
    start_date=datetime(2024, 6, 1),
    catchup=False,
) as dag:

    # Task 1: Wait for the file 'run' to appear in a  directory
    wait_for_file = FileSensor(
        task_id='wait_for_run_file',
        filepath='/opt/airflow/data/trigger_run.txt',  
        poke_interval=30,  
        timeout=600,  
        fs_conn_id='fs_default'  
    )

    # Task 2: Trigger the table update DAG once the file is detected
    trigger_table_update_dag = TriggerDagRunOperator(
        task_id='trigger_external_dag',
        trigger_dag_id='table_update_dag',  
        wait_for_completion=True 
    )

    # Task 3: Remove the 'run' file after the external DAG finishes
    remove_run_file = BashOperator(
        task_id='remove_run_file',
        bash_command='rm -f /opt/airflow/data/trigger_run.txt'  
    )

    
    wait_for_file >> trigger_table_update_dag >> remove_run_file
