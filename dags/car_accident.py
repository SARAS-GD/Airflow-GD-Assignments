import pandas as pd
from datetime import datetime, timedelta
from airflow.sdk import DAG, task

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

FILEPATH = '/opt/airflow/data/monroe-county-crash.csv'

with DAG(
    'etl_car_accident_data',
    default_args=default_args,
    schedule=None,
) as dag:

    @task
    def download_data(filepath: str = FILEPATH) -> str:
        df = pd.read_csv(filepath, encoding='ISO-8859-1')
        print(df.head())
        # Return only the path, not the data
        return filepath

    @task
    def count_accidents_per_year(filepath: str) -> dict:
        df = pd.read_csv(filepath, encoding='ISO-8859-1')
        df.columns = df.columns.str.strip().str.lower()

        if 'year' not in df.columns:
            raise KeyError("'year' column not found in the data")

        counts = df.groupby('year').size()
        return {str(year): int(count) for year, count in counts.items()}

    @task
    def print_results(accidents_per_year: dict) -> None:
        print("Accidents per year:")
        for year, count in accidents_per_year.items():
            print(f"Year: {year}, Accidents: {count}")

    path = download_data()
    counts = count_accidents_per_year(path)
    print_results(counts)
