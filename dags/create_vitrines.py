from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from sqlalchemy import create_engine, text

POSTGRES_PARAMS = {
    'host': 'postgres',
    'port': 5432,
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow'
}

# SQL для создания витрин
CREATE_VITRINES_SQL = """
-- Витрина 1: Топ-10 товаров за опредленный месяц (последний, по которому есть записи)
DROP TABLE IF EXISTS top_products_last_month;

CREATE TABLE top_products_last_month AS
WITH last_month AS (
    SELECT MAX(DATE_TRUNC('month', order_date)) AS month
    FROM fact_orders
),
monthly_sales AS (
    SELECT 
        oi.product_id,
        SUM(oi.quantity) AS monthly_quantity,
        SUM(oi.quantity * p.price) AS monthly_revenue
    FROM fact_order_items oi
    JOIN fact_orders o ON oi.order_id = o.order_id
    JOIN dim_products p ON oi.product_id = p.product_id
    CROSS JOIN last_month lm
    WHERE DATE_TRUNC('month', o.order_date) = lm.month
    GROUP BY oi.product_id
),
total_sales AS (
    SELECT 
        oi.product_id,
        SUM(oi.quantity) AS total_quantity,
        SUM(oi.quantity * p.price) AS total_revenue
    FROM fact_order_items oi
    JOIN fact_orders o ON oi.order_id = o.order_id
    JOIN dim_products p ON oi.product_id = p.product_id
    GROUP BY oi.product_id
)
SELECT 
    p.product_id,
    p.name AS product_name,
    p.category,
    ROUND(COALESCE(AVG(r.rating), 0), 2) AS avg_rating,
    COALESCE(ms.monthly_quantity, 0) AS last_month_sales,
    COALESCE(ms.monthly_revenue, 0) AS last_month_revenue,
    COALESCE(ts.total_quantity, 0) AS total_sales_all_time,
    COALESCE(ts.total_revenue, 0) AS total_revenue_all_time
FROM dim_products p
LEFT JOIN monthly_sales ms ON p.product_id = ms.product_id
LEFT JOIN total_sales ts ON p.product_id = ts.product_id
LEFT JOIN fact_reviews r ON p.product_id = r.product_id
GROUP BY p.product_id, p.name, p.category, ms.monthly_quantity, 
         ms.monthly_revenue, ts.total_quantity, ts.total_revenue
ORDER BY last_month_sales DESC
LIMIT 10;

-- Витрина 2: Топ-10 активных пользователей за конкретный месяц (последний, по которому есть записи)
DROP TABLE IF EXISTS top_users_last_month;

CREATE TABLE top_users_last_month AS
WITH last_month AS (
    SELECT MAX(DATE_TRUNC('month', order_date)) AS month
    FROM fact_orders
),
user_monthly_stats AS (
    SELECT 
        o.user_id,
        COUNT(oi.product_id) AS items_purchased,
        SUM(oi.quantity * p.price) AS total_spent,
        COUNT(DISTINCT o.order_id) AS orders_count
    FROM fact_orders o
    JOIN fact_order_items oi ON o.order_id = oi.order_id
    JOIN dim_products p ON oi.product_id = p.product_id
    CROSS JOIN last_month lm
    WHERE DATE_TRUNC('month', o.order_date) = lm.month
    GROUP BY o.user_id
),
user_monthly_reviews AS (
    SELECT 
        r.user_id,
        COUNT(r.review_id) AS reviews_count_last_month,
        AVG(r.rating) AS avg_rating_last_month
    FROM fact_reviews r
    CROSS JOIN last_month lm
    WHERE DATE_TRUNC('month', r.created_date) = lm.month
    GROUP BY r.user_id
),
user_total_reviews AS (
    SELECT 
        r.user_id,
        COUNT(r.review_id) AS total_reviews
    FROM fact_reviews r
    GROUP BY r.user_id
)
SELECT 
    u.user_id,
    u.name AS user_name,
    u.city,
    COALESCE(ums.items_purchased, 0) AS items_purchased_last_month,
    COALESCE(ums.total_spent, 0) AS total_spent_last_month,
    COALESCE(ums.orders_count, 0) AS orders_count_last_month,
    COALESCE(umr.reviews_count_last_month, 0) AS reviews_last_month,
    ROUND(COALESCE(umr.avg_rating_last_month, 0), 2) AS avg_rating_last_month,
    COALESCE(utr.total_reviews, 0) AS total_reviews_all_time
FROM dim_users u
LEFT JOIN user_monthly_stats ums ON u.user_id = ums.user_id
LEFT JOIN user_monthly_reviews umr ON u.user_id = umr.user_id
LEFT JOIN user_total_reviews utr ON u.user_id = utr.user_id
WHERE ums.items_purchased > 0
ORDER BY items_purchased_last_month DESC
LIMIT 10;
"""

def check_data_exists():
    """Проверяет, что данные загружены"""
    engine = create_engine(
        f"postgresql://{POSTGRES_PARAMS['user']}:{POSTGRES_PARAMS['password']}@"
        f"{POSTGRES_PARAMS['host']}:{POSTGRES_PARAMS['port']}/{POSTGRES_PARAMS['database']}"
    )
    
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM fact_orders"))
        count = result.scalar()
        
        if count == 0:
            raise Exception("Нет данных в fact_orders. Сначала запусти DAG replicate_mongo_to_postgres")
    
    engine.dispose()
    print(f"✅ Данные есть: {count} заказов")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

with DAG('create_vitrines',
    default_args=default_args,
    description='Создание аналитических витрин',
    schedule_interval=None,
    catchup=False,
    tags=['vitrines', 'analytics'],
) as dag:
    
    check_data = PythonOperator(
        task_id='check_data_exists',
        python_callable=check_data_exists
    )
    
    create_vitrines = PostgresOperator(
        task_id='create_vitrines',
        postgres_conn_id='postgres_default',
        sql=CREATE_VITRINES_SQL
    )
    
    check_data >> create_vitrines
