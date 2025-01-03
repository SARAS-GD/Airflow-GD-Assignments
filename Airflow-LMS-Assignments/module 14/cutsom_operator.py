from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults

class PostgreSQLCountRows(BaseOperator):
    @apply_defaults
    def __init__(self, table_name, postgres_conn_id='postgres_default', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        result = hook.get_records(f"SELECT COUNT(*) FROM {self.table_name};")
        row_count = result[0][0] if result else 0
        self.log.info(f"Row count for table {self.table_name}: {row_count}")
        return row_count
