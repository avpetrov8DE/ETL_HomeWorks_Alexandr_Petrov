from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os

CSV_PATH = '/opt/airflow/data/IOT-temp.csv'

DB_PARAMS = {
    'host': 'postgres',
    'port': 5432,
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow'
}

def process_weather_data():
    
    print("Чтение CSV файла...")
    df = pd.read_csv(CSV_PATH)
    print(f"Всего записей: {len(df)}")
    
    # Фильтрация 'In'
    df = df[df['out/in'] == 'In'].copy()
    print(f"После фильтрации In: {len(df)} записей")
    
    # Форматирование даты
    df['noted_date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M')
    df['date_only'] = df['noted_date'].dt.date
    
    # Очистка значений температуры по процентилю
    lower_bound = df['temp'].quantile(0.05)
    upper_bound = df['temp'].quantile(0.95)
    print(f"Нижняя граница (5%): {lower_bound}")
    print(f"Верхняя граница (95%): {upper_bound}")
    df_cleaned = df[(df['temp'] >= lower_bound) & (df['temp'] <= upper_bound)].copy()
    print(f"После очистки по процентилям: {len(df_cleaned)} записей")
    
    #Группировка по дням (средняя температура за день)
    daily_avg = df_cleaned.groupby('date_only')['temp'].mean().reset_index()
    daily_avg.columns = ['noted_date', 'avg_temp']
    print(f"Уникальных дней: {len(daily_avg)}")
    
    # 5 самых жарких дней
    hottest = daily_avg.nlargest(5, 'avg_temp').reset_index(drop=True)
    hottest['day_type'] = 'hottest'
    hottest['rank_num'] = range(1, 6)
    
    # 5 самых холодных дней
    coldest = daily_avg.nsmallest(5, 'avg_temp').reset_index(drop=True)
    coldest['day_type'] = 'coldest'
    coldest['rank_num'] = range(1, 6)
    
    extremes = pd.concat([hottest, coldest], ignore_index=True)
    
    print("\n=== 5 самых жарких дней ===")
    print(hottest[['noted_date', 'avg_temp']])
    
    print("\n=== 5 самых холодных дней ===")
    print(coldest[['noted_date', 'avg_temp']])
    
    return daily_avg, extremes

def save_to_postgres(**context):
   
    ti = context['task_instance']
    daily_avg, extremes = ti.xcom_pull(task_ids='process_data')
    
    # Подключение к бд
    engine = create_engine(
        f"postgresql://{DB_PARAMS['user']}:{DB_PARAMS['password']}@"
        f"{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['database']}"
    )
    
    # Очищенные данные
    daily_avg.to_sql('iot_temp_cleaned', engine, 
                     if_exists='replace', index=False)
    print(f"Сохранено {len(daily_avg)} записей в iot_temp_cleaned")
    
    # Экстремумы
    extremes.to_sql('temp_extremes', engine, 
                    if_exists='replace', index=False)
    print(f"Сохранено {len(extremes)} записей в temp_extremes")
    
    engine.dispose()

# Создаем DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

with DAG('weather_etl',
    default_args=default_args,
    description='ETL для обработки температурных данных',
    schedule_interval=None,
    catchup=False,
    tags=['weather', 'pandas'],
) as dag:
    
    process_task = PythonOperator(
        task_id='process_data',
        python_callable=process_weather_data
    )
    
    save_task = PythonOperator(
        task_id='save_to_postgres',
        python_callable=save_to_postgres
    )
    
    process_task >> save_task