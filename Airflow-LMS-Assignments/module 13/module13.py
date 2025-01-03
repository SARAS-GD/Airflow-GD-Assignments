from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator  # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import random

# Define table name
TABLE_NAME = "custom_table"

# Python callable to check if table exists
def check_table_exists():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    result = hook.get_records(f"SELECT to_regclass('{TABLE_NAME}');")
    return 'create_table' if result[0][0] is None else 'dummy_task'

# Python callable to get row count
def query_table():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    result = hook.get_records(f"SELECT COUNT(*) FROM {TABLE_NAME};")
    print(f"Row count: {result[0][0]}")
    return result[0][0]

# Define the DAG
with DAG(
    'modified_table_dag',
    start_date=days_ago(1),
    schedule_interval=None, 
    catchup=False
) as dag:
    
    # Start process message
    print_start = BashOperator(
        task_id='print_process_start',
        bash_command='echo "Process started"'
    )

    # Task to get the current user
    get_current_user = BashOperator(
        task_id='get_current_user',
        bash_command='whoami',
        do_xcom_push=True
    )
    
    # Branch task to check if the table exists
    check_table = BranchPythonOperator(
        task_id='check_table_exist',
        python_callable=check_table_exists
    )
    
    # Task to create the table if it doesn't exist
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_default',
        sql=f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            custom_id INTEGER NOT NULL,
            user_name VARCHAR(50) NOT NULL,
            timestamp TIMESTAMP NOT NULL
        );
        """
    )
    
    # Dummy task if the table already exists
    dummy_task = BashOperator(
        task_id='dummy_task',
        bash_command='echo "Table already exists"'
    )
    
    # Task to insert a row into the table
    insert_row = PostgresOperator(
        task_id='insert_row',
        postgres_conn_id='postgres_default',
        sql=f"""
        INSERT INTO {TABLE_NAME} (custom_id, user_name, timestamp)
        VALUES (%(custom_id)s, %(user_name)s, %(timestamp)s);
        """,
        parameters={
            'custom_id': random.randint(1, 1000000),
            'user_name': "{{ ti.xcom_pull(task_ids='get_current_user') }}",
            'timestamp': datetime.now()
        },
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS  # Ensures it runs even if one branch is skipped
    )
    
    # Task to query the table and get row count
    query_table_task = PythonOperator(
        task_id='query_table',
        python_callable=query_table
    )
    
    # Define the task dependencies
    print_start >> get_current_user >> check_table
    check_table >> [create_table, dummy_task]
    create_table >> insert_row >> query_table_task
    dummy_task >> insert_row
