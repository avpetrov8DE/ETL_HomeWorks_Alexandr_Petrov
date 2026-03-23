from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

with DAG('extract', 
    schedule_interval=timedelta(minutes=30),
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    
    extract_json = PostgresOperator(
        task_id='extract_json',
        postgres_conn_id='postgres_default',
        sql="""\
        CREATE TABLE IF NOT EXISTS public.data_from_json AS
        SELECT
            post.value->>'name' as name,
            post.value->>'species' as species,
            post.value->>'favFoods' as favFoods,
            post.value->>'birthYear' as birthYear,
            post.value->>'photo' as photo
        FROM public.json_storage,
        jsonb_array_elements(json_data->'pets') as post(value);
        """
    )