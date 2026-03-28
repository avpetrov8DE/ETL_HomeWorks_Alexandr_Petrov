from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine, text

MONGO_URI = 'mongodb://admin:admin123@host.docker.internal:27017/'
POSTGRES_PARAMS = {
    'host': 'postgres',
    'port': 5432,
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow'
}

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS dim_users (
    user_id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    city VARCHAR(50),
    registration_date DATE
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_id INTEGER PRIMARY KEY,
    name VARCHAR(200),
    category VARCHAR(50),
    price NUMERIC
);

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id INTEGER PRIMARY KEY,
    user_id INTEGER,
    order_date DATE,
    status VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS fact_order_items (
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    PRIMARY KEY (order_id, product_id)
);

CREATE TABLE IF NOT EXISTS fact_reviews (
    review_id INTEGER PRIMARY KEY,
    product_id INTEGER,
    user_id INTEGER,
    rating INTEGER,
    comment TEXT,
    created_date DATE
);
"""

def replicate_data():
    """Репликация данных из MongoDB в PostgreSQL с трансформацией"""
    
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client['ecommerce']
    
    engine = create_engine(
        f"postgresql://{POSTGRES_PARAMS['user']}:{POSTGRES_PARAMS['password']}@"
        f"{POSTGRES_PARAMS['host']}:{POSTGRES_PARAMS['port']}/{POSTGRES_PARAMS['database']}"
    )
    
    print("=" * 50)
    print("Репликация данных")
    print("=" * 50)
    
    # ========== 1. Продукты ==========
    print("\n1. Загрузка продуктов...")
    products_df = pd.DataFrame(list(db.products.find({}, {'_id': 0})))
    
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE dim_products"))
    products_df.to_sql('dim_products', engine, if_exists='append', index=False)
    print(f"   Загружено {len(products_df)} продуктов")
    
    # ========== 2. Пользователи (только с email) ==========
    print("\n2. Загрузка пользователей...")
    users_df = pd.DataFrame(list(db.users.find({}, {'_id': 0})))
    users_df = users_df[users_df['email'].notna() & (users_df['email'] != '')]
    
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE dim_users"))
    users_df.to_sql('dim_users', engine, if_exists='append', index=False)
    print(f"   Загружено {len(users_df)} пользователей")
    
    # ========== 3. Заказы (только completed и пользователи с email) ==========
    print("\n3. Загрузка заказов...")
    orders_df = pd.DataFrame(list(db.orders.find({}, {'_id': 0})))
    
    # Только completed
    orders_df = orders_df[orders_df['status'] == 'completed']
    print(f"   После фильтра completed: {len(orders_df)}")
    
    # Только пользователи с email
    valid_users = set(users_df['user_id'])
    orders_df = orders_df[orders_df['user_id'].isin(valid_users)]
    print(f"   После фильтра по пользователям: {len(orders_df)}")
    
    # Удаляем дубли order_id (оставляем последний по дате)
    orders_df = orders_df.sort_values('order_date').drop_duplicates('order_id', keep='last')
    print(f"   После удаления дублей: {len(orders_df)}")
    
    # fact_orders
    fact_orders = orders_df[['order_id', 'user_id', 'order_date', 'status']].copy()
    
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE fact_orders"))
    fact_orders.to_sql('fact_orders', engine, if_exists='append', index=False)
    print(f"   Загружено {len(fact_orders)} заказов")
    
    # fact_order_items
    order_items = []
    for _, row in orders_df.iterrows():
        order_id = row['order_id']
        for item in row['items']:
            order_items.append({
                'order_id': order_id,
                'product_id': item['product_id'],
                'quantity': item['quantity']
            })
    
    order_items_df = pd.DataFrame(order_items)
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE fact_order_items"))
    order_items_df.to_sql('fact_order_items', engine, if_exists='append', index=False)
    print(f"   Загружено {len(order_items_df)} записей в fact_order_items")
    
    # ========== 4. Отзывы ==========
    print("\n4. Загрузка отзывов...")
    reviews_df = pd.DataFrame(list(db.reviews.find({}, {'_id': 0})))
    
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE fact_reviews"))
    reviews_df.to_sql('fact_reviews', engine, if_exists='append', index=False)
    print(f"   Загружено {len(reviews_df)} отзывов")
    
    mongo_client.close()
    engine.dispose()
    
    print("\n" + "=" * 50)
    print("Репликация завершена")
    print("=" * 50)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

with DAG('final_etl',
    default_args=default_args,
    description='Финальный ETL: MongoDB → PostgreSQL',
    schedule_interval=None,
    catchup=False,
    tags=['final', 'etl', 'mongodb'],
) as dag:
    
    create_tables = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='postgres_default',
        sql=CREATE_TABLES_SQL
    )
    
    replicate = PythonOperator(
        task_id='replicate_data',
        python_callable=replicate_data
    )
    
    create_tables >> replicate
