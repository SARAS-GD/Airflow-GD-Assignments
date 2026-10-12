from datetime import datetime

from airflow.sdk import dag, task, task_group, get_current_context
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.filesystem import FileSensor

TARGET_DAG_ID = "table_update_dag_2"   # must exist and be unpaused
RUN_FILE = "/opt/airflow/data/trigger_run.txt"


@dag(
    dag_id="trigger_dag_with_slack_notification",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "airflow", "retries": 1},
)
def trigger_dag_with_slack_notification():

    # Task 1: wait for the "run" file
    wait_for_run_file = FileSensor(
        task_id="wait_for_run_file",
        filepath=RUN_FILE,
        fs_conn_id="fs_default",
        poke_interval=10,
        timeout=600,
        mode="reschedule",
    )

    # Task 2: trigger the other DAG and wait for it to finish
    trigger_dag_task = TriggerDagRunOperator(
        task_id="trigger_dag_task",
        trigger_dag_id=TARGET_DAG_ID,
        wait_for_completion=True,
        poke_interval=10,
    )

    # Task 3: process results
    @task_group(group_id="process_results_task")
    def process_results_task():

        @task
        def print_result():
            context = get_current_context()
            ti = context["ti"]

            triggered_run_id = ti.xcom_pull(
                task_ids="trigger_dag_task", key="trigger_run_id"
            )

            result = None
            if triggered_run_id:
                result = ti.xcom_pull(
                    dag_id=TARGET_DAG_ID,
                    task_ids="end_task",
                    key="result",
                    run_id=triggered_run_id,
                )

            print(f"Triggered run id: {triggered_run_id}")
            print(f"Result from {TARGET_DAG_ID}: {result}")
            print(f"Data Interval Start: {context.get('data_interval_start')}")
            print(f"Logical Date: {context.get('logical_date')}")

        remove_run_file = BashOperator(
            task_id="remove_run_file",
            bash_command=f"rm -f {RUN_FILE}",
        )

        create_finished_file = BashOperator(
            task_id="create_finished_file",
            bash_command="touch /opt/airflow/data/finished_{{ ts_nodash }}.txt",
        )

        print_result() >> remove_run_file >> create_finished_file

    # Task 4: Slack notification via incoming webhook
    @task
    def send_slack_notification():
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

        context = get_current_context()
        run_date = context.get("logical_date") or context["run_id"]
        message = f"DAG ID: {context['dag'].dag_id}, Execution Date: {run_date}"

        SlackWebhookHook(slack_webhook_conn_id="SLACK_CONN").send(text=message)

    wait_for_run_file >> trigger_dag_task >> process_results_task() >> send_slack_notification()


trigger_dag_with_slack_notification()   # registers the DAG, don't remove
