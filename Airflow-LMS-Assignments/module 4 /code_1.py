from datetime import datetime,timedelta

from airflow import DAG 
from airflow.operators.bash import BashOperator



config = {
    'dag_id_1': {'schedule_interval': '@daily', "start_date": datetime(2024, 1, 1)},  
    'dag_id_2': {'schedule_interval': '@hourly', "start_date": datetime(2024, 2, 1)},  
    'dag_id_3':{'schedule_interval': None, "start_date": datetime(2024, 3, 1)}}

for dag_id, params in config.items():
    with DAG(
        dag_id=dag_id,
        schedule_interval=params['schedule_interval'],
        start_date=params['start_date'],
        catchup=False
    ) as dag:
        
        check_file = BashOperator(
            task_id=f'check_file_{dag_id}',
            bash_command='echo "Checking for trigger file..."'
        )

        run_process = BashOperator(
            task_id=f'run_process_{dag_id}',
            bash_command='echo "Running Spark job with parameters..."'
        )

        write_result = BashOperator(
            task_id=f'write_result_{dag_id}',
            bash_command='echo "Writing results to the database..."'
        )

        # Define task dependencies
        check_file >> run_process >> write_result
    
    globals()[dag_id] = dag

    