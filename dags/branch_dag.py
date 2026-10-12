from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

config = {
    'dag_id_1': {'schedule': '@daily',  'start_date': datetime(2024, 1, 1), 'table_name': 'table_name_1'},
    'dag_id_2': {'schedule': '@hourly', 'start_date': datetime(2024, 2, 1), 'table_name': 'table_name_2'},
    'dag_id_3': {'schedule': None,      'start_date': datetime(2024, 3, 1), 'table_name': 'table_name_3'},
}

def log_start_processing(dag_id, table_name):
    print(f"{dag_id} start processing tables in database: {table_name}")

def check_table_exist(table_name):
    print(f"Checking if {table_name} exists...")
    table_exists = True  # replace with a real check
    return 'skip_create_table' if table_exists else 'create_table'

for dag_id, params in config.items():
    with DAG(
        dag_id=dag_id,
        schedule=params['schedule'],
        start_date=params['start_date'],
        catchup=False,
    ) as dag:

        print_process_start = PythonOperator(
            task_id='print_process_start',
            python_callable=log_start_processing,
            op_args=[dag_id, params['table_name']],
        )

        get_current_user = BashOperator(
            task_id='get_current_user',
            bash_command='whoami',
        )

        check_table = BranchPythonOperator(
            task_id='check_table_exist',
            python_callable=check_table_exist,
            op_args=[params['table_name']],
        )

        create_table = EmptyOperator(task_id='create_table')
        skip_create_table = EmptyOperator(task_id='skip_create_table')

        insert_new_row = EmptyOperator(
            task_id='insert_new_row',
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        query_the_table = EmptyOperator(task_id='query_the_table')

        print_process_start >> get_current_user >> check_table
        check_table >> [create_table, skip_create_table]
        [create_table, skip_create_table] >> insert_new_row >> query_the_table

    globals()[dag_id] = dag
