"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
with DAG(
    dag_id='table_update_dag',
    default_args=default_args,
    description='A simple DAG for table updates',
    schedule_interval=None,
    start_date=datetime(2024, 6, 1),
    catchup=False,
) as dag:

    # Define tasks for the table update DAG
    start_task = EmptyOperator(task_id='start')

    # Define more tasks related to table update as needed...
    # table_update_task = SomeTableUpdateOperator(...)

    start_task  # Add more tasks as needed

"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Default arguments for DAGs
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='table_update_dag_2',
    default_args=default_args,
    description='A simple DAG for table updates',
    schedule_interval=None,
    start_date=datetime(2024, 6, 1),
    catchup=False,
) as dag:

    # Task 1: Start Task
    start_task = EmptyOperator(task_id='start')

    # Function to push XCom value at the end of the DAG
    def end_task_function(**context):
        context['ti'].xcom_push(key='result', value=f"{context['run_id']} ended")

    # Task 2: End Task
    end_task = PythonOperator(
        task_id='end_task',
        python_callable=end_task_function,
    )

    start_task >> end_task

