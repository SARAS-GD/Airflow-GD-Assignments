from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator



config = {
    'dag_id_1': {'schedule_interval': '@daily', "start_date": datetime(2024, 1, 1), "table_name": "table_name_1"},  
    'dag_id_2': {'schedule_interval': '@hourly', "start_date": datetime(2024, 2, 1), "table_name": "table_name_2"},  
    'dag_id_3':{'schedule_interval': None, "start_date": datetime(2024, 3, 1), "table_name": "table_name_3"}}

def log_start_processing(dag_id, table_name):
    print(f"{dag_id} start processing tables in database: {table_name}")

for dag_id, params in config.items():
    with DAG(
        dag_id=dag_id,
        schedule_interval=params['schedule_interval'],
        start_date=params['start_date'],
        catchup=False
    ) as dag:
        
        # Task 1: Log the start of table processing
        start_processing = PythonOperator(
            task_id='start_processing',
            python_callable=log_start_processing,
            op_args=[dag_id,params['table_name']]
        )
        
        # Task 2: Mock insertion of a new row into the database
        insert_new_row = EmptyOperator(
            task_id='insert_new_row'
        )
        
        # Task 3: Mock querying the table
        query_the_table = EmptyOperator(
            task_id='query_the_table'
        )
        
        # Define task dependencies
        start_processing >> insert_new_row >> query_the_table

    # Add DAG to the global namespace
    globals()[dag_id] = dag
