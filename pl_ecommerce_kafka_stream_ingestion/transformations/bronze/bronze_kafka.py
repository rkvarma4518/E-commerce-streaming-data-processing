from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


@dp.table(
    name="`e-commerce`.bronze.bronze_kafka_events",
    comment="Raw Kafka events from e-commerce topic",
)
def bronze_kafka_events():
    """Ingest raw events from Kafka and store in bronze layer."""
    kafka_brokers = spark.conf.get("kafka_brokers")
    kafka_topic = spark.conf.get("kafka_topic")
    username = spark.conf.get("kafka_username_key", "")
    password = spark.conf.get("kafka_password_secret", "")

    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", kafka_topic)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{username}" password="{password}";',
        )
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr(
            "CAST(key AS STRING) AS key",
            "CAST(value AS STRING) AS value",
            "topic",
            "partition",
            "offset",
            "timestamp AS kafka_timestamp",
            "timestampType",
            "current_timestamp() AS _ingested_at",
        )
    )
