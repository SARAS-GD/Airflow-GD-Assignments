from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# Config for DAGs
config = {
    'dag_id_1': {'schedule': '@daily', "start_date": datetime(2024, 1, 1), "table_name": "table_name_1"},
    'dag_id_2': {'schedule': '@hourly', "start_date": datetime(2024, 2, 1), "table_name": "table_name_2"},
    'dag_id_3': {'schedule': None, "start_date": datetime(2024, 3, 1), "table_name": "table_name_3"}
}

def log_start_processing(dag_id, table_name):
    """Log the start of table processing."""
    print(f"{dag_id} start processing tables in database: {table_name}")

def check_table_exist(table_name):
    """Checking if the table exists. If not,create it."""
    print(f"Checking if {table_name} exists...")
    # Simulate the table existence check (True means it exists, False means it doesn't)
    if True:  # Assume table exists for now
        return 'insert_new_row'
    return 'create_table'

for dag_id, params in config.items():
    with DAG(
        dag_id=dag_id,
        schedule=params['schedule'],
        start_date=params['start_date'],
        catchup=False
    ) as dag:

        # Task 1: Log the start of table processing
        start_processing = PythonOperator(
            task_id='start_processing',
            python_callable=log_start_processing,
            op_args=[dag_id, params['table_name']]
        )

        # Task 2: Check if the table exists
        check_table = BranchPythonOperator(
            task_id='check_table_exist',
            python_callable=check_table_exist,
            op_args=[params['table_name']]
        )

        # Task 3: Mock insertion of a new row into the database
        insert_new_row = EmptyOperator(
            task_id='insert_new_row',
            trigger_rule=TriggerRule.NONE_FAILED  # Allow this task to run even if it’s skipped upstream
        )

        # Task 4: Mock creating the table
        create_table = EmptyOperator(
            task_id='create_table',
            trigger_rule=TriggerRule.NONE_FAILED  # Allow this task to run even if it’s skipped upstream
        )

        # Task 5: Mock querying the table
        query_the_table = EmptyOperator(
            task_id='query_the_table',
            trigger_rule=TriggerRule.NONE_FAILED  # Allow this task to run even if it’s skipped upstream
        )

        start_processing >> check_table
        check_table >> create_table >> insert_new_row >> query_the_table  # If table doesn't exist, create it, then insert and query
        check_table >> insert_new_row >> query_the_table  # If table exists, directly insert and query


    # Register the DAG in the global namespace
    globals()[dag_id] = dag  # This makes the DAG available to Airflow.




        


