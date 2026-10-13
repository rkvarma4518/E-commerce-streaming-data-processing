from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


@dp.materialized_view(
    name="`e-commerce`.gold.dim_customers",
    comment="Customer dimension - unique customers with city",
)
def dim_customers():
    """Build customer dimension from silver events."""
    return (
        spark.read.table("`e-commerce`.silver.silver_events")
        .filter(F.col("user_id").isNotNull())
        .groupBy("user_id", "city")
        .agg(
            F.min("event_time").alias("first_seen"),
            F.max("event_time").alias("last_active"),
        )
        .select(
            F.col("user_id").alias("customer_id"),
            F.col("city"),
            F.col("first_seen"),
            F.col("last_active"),
        )
    )
