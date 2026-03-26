from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text

DB_PARAMS = {
    'host': 'postgres',
    'port': 5432,
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow'
}

TARGET_TABLE = 'daily_temps_warehouse'
DAYS_BACK = 7  

def incremental_load_from_cleaned():
    """Загружает только новые данные за последние 7 дней"""
    print("=" * 50)
    print(" ИНКРЕМЕНТАЛЬНАЯ ЗАГРУЗКА")
    print("=" * 50)
    
    engine = create_engine(
        f"postgresql://{DB_PARAMS['user']}:{DB_PARAMS['password']}@"
        f"{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['database']}"
    )
    
    # Определяем дату cutoff - последние 7 дней от текущей даты
    # Для демонстрации на данных 2018 года используем фиксированную дату
    # В продакшене: cutoff_date = datetime.now() - timedelta(days=DAYS_BACK)
    cutoff_date = datetime(2018, 12, 10) - timedelta(days=DAYS_BACK)
    print(f" Загружаем данные с {cutoff_date.strftime('%Y-%m-%d')} (последние {DAYS_BACK} дней)")
    
    # Забираем данные за последние 7 дней
    query = """
        SELECT * FROM iot_temp_cleaned 
        WHERE noted_date >= %(cutoff_date)s
        ORDER BY noted_date
    """
    
    df_new = pd.read_sql(query, engine, params={'cutoff_date': cutoff_date})
    print(f"   Найдено {len(df_new)} записей за последние {DAYS_BACK} дней")
    
    if len(df_new) == 0:
        print(" Нет данных для загрузки")
        engine.dispose()
        return
    
    # Удаляем старые записи за эти даты (чтобы обновить)
    print(f" Удаление старых записей за период...")
    with engine.connect() as conn:
        conn.execute(
            text(f"DELETE FROM {TARGET_TABLE} WHERE noted_date >= :cutoff_date"),
            {"cutoff_date": cutoff_date}
        )
    
    # Добавляем новые данные
    print(f" Добавление {len(df_new)} записей в {TARGET_TABLE}...")
    df_new.to_sql(TARGET_TABLE, engine, if_exists='append', index=False)
    
    print(f"   Загружено {len(df_new)} записей")
    print(f"   Диапазон дат: {df_new['noted_date'].min()} - {df_new['noted_date'].max()}")
    
    engine.dispose()
    print("\n ИНКРЕМЕНТАЛЬНАЯ ЗАГРУЗКА ЗАВЕРШЕНА")

incremental_dag = DAG(
    'incremental_load_from_cleaned',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2026, 1, 1),
        'retries': 1,
    },
    description='Инкрементальная загрузка очищенных данных (последние 7 дней)',
    schedule_interval='0 0 */7 * *',
    catchup=False,
    tags=['incremental', 'warehouse', 'scheduled'],
)

incremental_load_task = PythonOperator(
    task_id='incremental_load',
    python_callable=incremental_load_from_cleaned,
    dag=incremental_dag,
)

incremental_load_task