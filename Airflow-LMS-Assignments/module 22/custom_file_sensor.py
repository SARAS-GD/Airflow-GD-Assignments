from airflow.sensors.filesystem import FileSensor
class CustomFileSensor(FileSensor):
    # Add poke_context_fields to define required initialization fields
    poke_context_fields = ('filepath', 'fs_conn_id')