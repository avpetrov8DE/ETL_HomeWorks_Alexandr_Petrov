import uuid
import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

# Данные вашей инфраструктуры
YC_DP_AZ = 'ru-central1-a'
YC_DP_SSH_PUBLIC_KEY = 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDOUPanNw+kacwCOobhHCBjR0qt4JsKXTkWo0lZf7ChOV/hEs5aFJBHe+h1isKYXrzM+XEfYoF74JbLoGasgU1llkofQ9OWsbKHAc01nQELc8nfcsjxYe6qjY3seGl3Fyh/5et7+9Us0VBCwRfnzdzW681oWioGOOBQ7kS2zrnfF4sVESeUihfpYtwsxcqwN9m2r804AqvwVveF028vwANDm8tsxc7fb0W4yd6S7wBPxCbxoArLSz3WtUxb9R+f/jwIxLPTkFFUHtF688qB8lTU71knLix9EZH7OAuBVTOTlONxWRXuXdMOxdEufGTeU8KGlJwSVGe275R3gA5xh2xL pc@DESKTOP-GG37PUM'
YC_DP_SUBNET_ID = 'e9b4i08jasss86e5mqpo'
YC_DP_SA_ID = 'ajet2ia6ik9q2khb7lt4'
YC_DP_METASTORE_URI = '10.129.0.18'
YC_BUCKET = 'etl-dataproc-bucket-1'

# Настройки DAG
with DAG(
        'DATA_INGEST',
        schedule='@hourly',
        tags=['data-processing-and-airflow'],
        start_date=datetime.datetime.now(),
        max_active_runs=1,
        catchup=False
) as ingest_dag:
    # 1 этап: создание кластера Yandex Data Proc
    create_spark_cluster = DataprocCreateClusterOperator(
        task_id='dp-cluster-create-task',
        cluster_name=f'tmp-dp-{uuid.uuid4()}',
        cluster_description='Временный кластер для выполнения PySpark-задания под оркестрацией Managed Service for Apache Airflow™',
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        service_account_id=YC_DP_SA_ID,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_BUCKET,
        zone=YC_DP_AZ,
        cluster_image_version='2.1',
        masternode_resource_preset='s2.small',
        masternode_disk_type='network-ssd',
        masternode_disk_size=20,
        computenode_resource_preset='m2.large',
        computenode_disk_type='network-ssd',
        computenode_disk_size=20,
        computenode_count=2,
        computenode_max_hosts_count=5,  # Количество подкластеров для обработки данных будет автоматически масштабироваться в случае большой нагрузки.
        services=['YARN', 'SPARK'],     # Создается легковесный кластер.
        datanode_count=0,               # Без подкластеров для хранения данных.
        properties={                    # С указанием на удаленный кластер Apache Hive™ Metastore.
            'spark:spark.hive.metastore.uris': f'thrift://{YC_DP_METASTORE_URI}:9083',
        },
    )

    # 2 этап: запуск задания PySpark
    poke_spark_processing = DataprocCreatePysparkJobOperator(
        task_id='dp-cluster-pyspark-task',
        main_python_file_uri=f's3a://{YC_BUCKET}/scripts/test_spark.py',
    )

    # 3 этап: удаление кластера Yandex Data Processing
    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id='dp-cluster-delete-task',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Формирование DAG из указанных выше этапов
    create_spark_cluster >> poke_spark_processing >> delete_spark_cluster