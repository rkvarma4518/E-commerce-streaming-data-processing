from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", StringType()),
    StructField("user_id", StringType()),
    StructField("city", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
])


@dp.table(
    name="`e-commerce`.silver.silver_events",
    comment="Parsed and cleaned e-commerce events from bronze Kafka data",
)
@dp.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dp.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dp.expect_or_drop("valid_event_type", "event_type IS NOT NULL")
def silver_events():
    """Parse JSON payload from bronze and apply data quality expectations."""
    return (
        spark.readStream.table("`e-commerce`.bronze.bronze_kafka_events")
        .withColumn("data", F.from_json(F.col("value"), event_schema))
        .select(
            F.col("data.event_id").alias("event_id"),
            F.col("data.event_type").alias("event_type"),
            F.to_timestamp(F.col("data.event_time")).alias("event_time"),
            F.col("data.user_id").alias("user_id"),
            F.col("data.city").alias("city"),
            F.col("data.product_id").alias("product_id"),
            F.col("data.quantity").alias("quantity"),
            F.col("_ingested_at"),
        )
    )
