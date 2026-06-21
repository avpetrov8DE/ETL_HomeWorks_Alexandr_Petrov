import json
import random
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

def main():
    spark = SparkSession.builder \
        .appName("dataproc-kafka-writer") \
        .getOrCreate()

    KAFKA_BOOTSTRAP_SERVERS = "rc1b-md0r60tkni4l653o.mdb.yandexcloud.net:9091"
    KAFKA_TOPIC = "dataproc-kafka-topic"

    # Генерируем массив из ~45000 записей, чтобы суммарно вышло около 20 МБ данных
    raw_records = []
    for _ in range(45000):
        record = {
            "application_id": f"loan_{random.randint(100000, 999999)}",
            "customer": {
                "customer_id": f"cust_{random.randint(100, 999)}",
                "region": random.choice(["DE-HE", "DE-BY", "DE-SN", "DE-OST"])
            },
            "loan": {
                "amount": random.choice([15000, 25000, 50000, 100000, 350000]),
                "term_months": random.choice([12, 24, 36, 48, 60])
            },
            "scoring": {
                "score": random.randint(500, 850),
                "risk_level": random.choice(["low", "medium", "high"])
            },
            "documents": [
                {"type": random.choice(["passport", "driver_license"]), "status": "verified"}
            ],
            "decision_status": random.choice(["approved", "manual_review", "rejected"]),
            "submitted_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        raw_records.append(json.dumps(record))

    # Переводим массив строк в Spark DataFrame в колонку "value"
    df = spark.createDataFrame([(r,) for r in raw_records], ["value"])

    # Отправляем данные в Кафку через встроенный коннектор Spark
    df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", KAFKA_TOPIC) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.scram.ScramLoginModule required username="user1" password="password1";') \
        .save()  

    spark.stop()

if __name__ == "__main__":
    main()
