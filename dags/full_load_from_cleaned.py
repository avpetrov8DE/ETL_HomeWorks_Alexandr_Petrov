from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

DB_PARAMS = {
    'host': 'postgres',
    'port': 5432,
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow'
}

# Таблица для результатов
TARGET_TABLE = 'daily_temps_warehouse'

def full_load_from_cleaned():
    """Полная загрузка всех очищенных данных"""
    print("=" * 50)
    print(" ПОЛНАЯ ЗАГРУЗКА ДАННЫХ")
    print("=" * 50)
    
    engine = create_engine(
        f"postgresql://{DB_PARAMS['user']}:{DB_PARAMS['password']}@"
        f"{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['database']}"
    )
    
    # 1. Забираем все данные из результата прошлого преобразования (здесь хранится среднесуточная температура после очистки строк по правилам из прошлого дз) 
    print(" Чтение данных из iot_temp_cleaned...")
    df = pd.read_sql("SELECT * FROM iot_temp_cleaned ORDER BY noted_date", engine)
    print(f"   Прочитано {len(df)} записей")
    
    # 2. Очищаем целевую таблицу
    print(f"  Очистка таблицы {TARGET_TABLE}...")
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {TARGET_TABLE}"))
    print("   Таблица очищена")
    
    # 3. Загружаем новые данные
    print(f" Сохранение в {TARGET_TABLE}...")
    df.to_sql(TARGET_TABLE, engine, if_exists='append', index=False)
    print(f" Загружено {len(df)} записей")
        
    engine.dispose()
    print("\n ПОЛНАЯ ЗАГРУЗКА ЗАВЕРШЕНА")


full_load_dag = DAG(
    'full_load_from_cleaned',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2026, 1, 1),
        'retries': 1,
    },
    description='Полная загрузка очищенных данных в витрину',
    schedule_interval=None, 
    catchup=False,
    tags=['full_load', 'warehouse', 'manual'],
)

full_load_task = PythonOperator(
    task_id='full_load',
    python_callable=full_load_from_cleaned,
    dag=full_load_dag,
)

full_load_task