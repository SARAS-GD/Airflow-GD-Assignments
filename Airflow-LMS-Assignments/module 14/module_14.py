from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import random

# Import custom operator
from custom_operator_module_14 import PostgreSQLCountRows

TABLE_NAME = "custom_table"


# Function to check table existence
def check_table_exists():
    hook = PostgresHook(postgres_conn_id='postgres_default')

    result = hook.get_records(
        f"SELECT to_regclass('{TABLE_NAME}');"
    )

    return 'create_table' if result[0][0] is None else 'dummy_task'


with DAG(
    dag_id='modified_table_dag_with_custom_operator',
    start_date=datetime.now() - timedelta(days=1),
    schedule=None,
    catchup=False
) as dag:

    # Start task
    print_start = BashOperator(
        task_id='print_process_start',
        bash_command='echo "Process started"'
    )

    # Get current user
    get_current_user = BashOperator(
        task_id='get_current_user',
        bash_command='whoami',
        do_xcom_push=True
    )

    # Branch task
    check_table = BranchPythonOperator(
        task_id='check_table_exist',
        python_callable=check_table_exists
    )

    # Create table task
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgres_default',
        sql=f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            custom_id INTEGER NOT NULL,
            user_name VARCHAR(50) NOT NULL,
            timestamp TIMESTAMP NOT NULL
        );
        """
    )

    # Dummy task
    dummy_task = BashOperator(
        task_id='dummy_task',
        bash_command='echo "Table already exists"'
    )

    # Insert row task
    insert_row = SQLExecuteQueryOperator(
        task_id='insert_row',
        conn_id='postgres_default',
        sql=f"""
        INSERT INTO {TABLE_NAME}
        (custom_id, user_name, timestamp)
        VALUES
        (%(custom_id)s, %(user_name)s, %(timestamp)s);
        """,
        parameters={
            'custom_id': random.randint(1, 1000000),
            'user_name': "{{ ti.xcom_pull(task_ids='get_current_user') }}",
            'timestamp': datetime.now()
        },
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # Custom operator task
    query_table_task = PostgreSQLCountRows(
        task_id='query_table',
        table_name=TABLE_NAME,
        postgres_conn_id='postgres_default'
    )

    # Dependencies
    print_start >> get_current_user >> check_table

    check_table >> [create_table, dummy_task]

    create_table >> insert_row
    dummy_task >> insert_row

    insert_row >> query_table_task
