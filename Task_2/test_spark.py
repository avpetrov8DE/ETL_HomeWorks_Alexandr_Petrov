from pyspark.sql import SparkSession
from pyspark.sql.types import *

# 1. Инициализация Spark-сессии с поддержкой Hive Metastore
spark = SparkSession.builder \
    .appName("process-loan-applications") \
    .enableHiveSupport() \
    .getOrCreate()

# 2. Строгое описание схемы данных на основе структуры вашего CSV
schema = StructType([
    StructField('application_id', StringType(), True),
    StructField('event_time', TimestampType(), True),          # Распарсит строку даты в TIMESTAMP
    StructField('customer_id', StringType(), True),
    StructField('region_code', StringType(), True),
    StructField('product_type', StringType(), True),
    StructField('requested_amount', IntegerType(), True),      # Целое число
    StructField('term_months', IntegerType(), True),
    StructField('credit_score', IntegerType(), True),
    StructField('risk_level', StringType(), True),
    StructField('decision_status', StringType(), True),
    StructField('approved_amount', IntegerType(), True),
    StructField('channel', StringType(), True),
    StructField('employee_review_flag', BooleanType(), True),  # Распарсит true/false в BOOLEAN
    StructField('processing_time_sec', IntegerType(), True)
])

# Пути к данным внутри бакета
INPUT_CSV_PATH = "s3a://etl-dataproc-bucket-1/input/loan_applications.csv"
OUTPUT_TABLE_PATH = "s3a://etl-dataproc-bucket-1/loan_applications_table"

# 3. Чтение CSV-файла с применением схемы
df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load(INPUT_CSV_PATH)

# 4. Запись данных в Hive Metastore под именем loan_applications
# Физические данные будут сохранены в формате Parquet по указанному пути
df.write \
    .mode("overwrite") \
    .option("path", OUTPUT_TABLE_PATH) \
    .saveAsTable("loan_applications")

# Завершение сессии
spark.stop()
