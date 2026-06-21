from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

def main():
    spark = SparkSession.builder \
        .appName("dataproc-kafka-read-stream") \
        .getOrCreate()

    KAFKA_BOOTSTRAP_SERVERS = "rc1b-md0r60tkni4l653o.mdb.yandexcloud.net:9091"
    KAFKA_TOPIC = "dataproc-kafka-topic"
    BUCKET_NAME = "dataproc-bucket-task3" 

    json_schema = StructType([
        StructField('application_id', StringType(), True),
        StructField('customer', StructType([
            StructField('customer_id', StringType(), True),
            StructField('region', StringType(), True)
        ]), True),
        StructField('loan', StructType([
            StructField('amount', IntegerType(), True),
            StructField('term_months', IntegerType(), True)
        ]), True),
        StructField('scoring', StructType([
            StructField('score', IntegerType(), True),
            StructField('risk_level', StringType(), True)
        ]), True),
        StructField('documents', ArrayType(StructType([
            StructField('type', StringType(), True),
            StructField('status', StringType(), True)
        ])), True),
        StructField('decision_status', StringType(), True),
        StructField('submitted_at', StringType(), True)
    ])

    raw_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.scram.ScramLoginModule required username="user1" password="password1";') \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_stream_df = raw_stream_df \
        .selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), json_schema).alias("data")) \
        .select("data.*")

    flat_stream_df = parsed_stream_df \
        .select(
            col("application_id"),
            col("customer.customer_id").alias("customer_id"),
            col("customer.region").alias("region"),
            col("loan.amount").alias("loan_amount"),
            col("loan.term_months").alias("loan_term_months"),
            col("scoring.score").alias("scoring_score"),
            col("scoring.risk_level").alias("scoring_risk_level"),
            col("documents").getItem(0).getField("type").alias("document_type"),
            col("documents").getItem(0).getField("status").alias("document_status"),
            col("decision_status"),
            col("submitted_at")
        )

    # Сохраняем в папку kafka-read-stream-output
    query = flat_stream_df.writeStream \
        .format("parquet") \
        .option("path", f"s3a://{BUCKET_NAME}/kafka-read-stream-output") \
        .option("checkpointLocation", f"s3a://{BUCKET_NAME}/checkpoints/kafka-read-stream-output") \
        .trigger(once=True) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
