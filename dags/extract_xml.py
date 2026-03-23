from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

with DAG('extract_xml', 
    schedule_interval=timedelta(minutes=30),
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    
    extract_xml = PostgresOperator(
        task_id='extract_xml',
        postgres_conn_id='postgres_default',
        sql="""\
        CREATE TABLE IF NOT EXISTS public.food_data AS
        SELECT
            (xpath('/food/name/text()', food_data))[1]::text as name,
            (xpath('/food/mfr/text()', food_data))[1]::text as manufacturer,
            (xpath('/food/serving/text()', food_data))[1]::text as serving_size,
            (xpath('/food/serving/@units', food_data))[1]::text as serving_units,
            (xpath('/food/calories/@total', food_data))[1]::text::integer as calories_total,
            (xpath('/food/total-fat/text()', food_data))[1]::text::float as total_fat
        FROM public.xml_storage,
        unnest(xpath('/nutrition/food', xml_data)) as food_data;
        """
    )