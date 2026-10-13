from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


@dp.materialized_view(
    name="`e-commerce`.gold.fact_engagement",
    comment="Fact table - customer engagement events with FKs to dimensions",
)
def fact_engagement():
    """Build fact table from silver events with foreign keys to dimension tables."""
    return (
        spark.read.table("`e-commerce`.silver.silver_events")
        .filter(F.col("event_id").isNotNull())
        .withColumn("date_key", F.date_format(F.col("event_time"), "yyyyMMdd").cast("int"))
        .select(
            F.col("event_id"),
            F.col("user_id").alias("customer_id"),
            F.col("product_id"),
            F.col("event_type"),
            F.col("event_time"),
            F.col("date_key"),
            F.col("quantity"),
        )
    )
