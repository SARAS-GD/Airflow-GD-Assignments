from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from slack import WebClient
from slack.errors import SlackApiError
from airflow.hooks.base import BaseHook # type: ignore
from custom_file_sensor_module_22 import CustomFileSensor


# Slack notification function
def send_slack_notification(**kwargs):
    # Get the Slack token from the Airflow connection
    slack_token = BaseHook.get_connection("slack_connection").extra_dejson.get("token")
    client = WebClient(token=slack_token)

    # Define the Slack message
    message = f"DAG ID: {kwargs['dag'].dag_id}, Execution Date: {kwargs['execution_date']}"

    try:
        response = client.chat_postMessage(
            channel="#airflow-channel-saras",
            text=message
        )
    except SlackApiError as e:
        # Handle Slack API errors
        assert e.response["error"]

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='trigger_dag_with_slack_notification',
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

   
    # Task 1: Sensor to wait for the "run" file
    # wait_for_run_file = FileSensor(
    #     task_id='wait_for_run_file',
    #     filepath='/opt/airflow/data/trigger_run.txt',  # Replace with actual file path
    #     fs_conn_id='fs_default',
    #     poke_interval=10,
    #     timeout=600,
    # )
    file_sensor_task = CustomFileSensor(
        task_id='wait_for_run_file',
        filepath='/opt/airflow/data/trigger_run.txt',
        fs_conn_id='fs_default',
        poke_interval=30,
        timeout=600
    )

    # Task 2: Trigger another DAG
    trigger_dag_task = TriggerDagRunOperator(
        task_id='trigger_dag_task',
        trigger_dag_id='table_update_dag_2',  # Replace with your target DAG ID
        wait_for_completion=True,
    )

    # Task 3: TaskGroup for processing results
    with TaskGroup(group_id='process_results_task') as process_results_task:

        # Print result using PythonOperator
        def print_result(**context):
            result = context['ti'].xcom_pull(
                dag_id='table_update_dag_2', 
                task_ids='end_task',
                key='result'
            )
            print(f"Result from table_update_dag_2: {result}")
            print("Execution Context:")
            print(f"Data Interval Start: {context.get('data_interval_start')}")
            print(f"Logical Date: {context.get('logical_date')}")

        print_result_task = PythonOperator(
            task_id='print_result',
            python_callable=print_result
        )

        # Remove 'run' file
        remove_run_file = BashOperator(
            task_id='remove_run_file',
            bash_command='rm -f /opt/airflow/data/trigger_run.txt'
        )

        # Create 'finished_#timestamp' file
        create_finished_file = BashOperator(
            task_id='create_finished_file',
            bash_command='touch /opt/airflow/data/finished_{{ ts_nodash }}.txt'
        )

        # Task dependencies within TaskGroup
        print_result_task >> remove_run_file >> create_finished_file

    # Task 4: Send notification to Slack
    alert_to_slack = PythonOperator(
        task_id="send_slack_notification",
        python_callable=send_slack_notification,
    )

    # Set task dependencies
    file_sensor_task >> trigger_dag_task >> process_results_task >> alert_to_slack
