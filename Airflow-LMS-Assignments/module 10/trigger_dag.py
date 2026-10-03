from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.bash import BashOperator

FILE_PATH = '/opt/airflow/data/trigger_run.txt'

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='trigger_table_update_dag',
    default_args=default_args,
    description='Waits for a file, triggers another DAG, then removes the file',
    schedule=None,
    start_date=datetime(2024, 6, 1),
    catchup=False,
) as dag:

    wait_for_file = FileSensor(
        task_id='wait_for_run_file',
        filepath=FILE_PATH,
        fs_conn_id='fs_default',
        poke_interval=30,
        timeout=600,
        mode='reschedule',
    )

    trigger_table_update_dag = TriggerDagRunOperator(
        task_id='trigger_external_dag',
        trigger_dag_id='table_update_dag',
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
    )

    remove_run_file = BashOperator(
        task_id='remove_run_file',
        bash_command=f'rm -f {FILE_PATH}',
        trigger_rule='all_done',
    )

    wait_for_file >> trigger_table_update_dag >> remove_run_file
