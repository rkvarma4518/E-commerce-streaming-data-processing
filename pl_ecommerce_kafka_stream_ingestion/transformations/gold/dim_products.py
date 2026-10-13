from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


@dp.materialized_view(
    name="`e-commerce`.gold.dim_products",
    comment="Product dimension - unique products",
)
def dim_products():
    """Build product dimension from silver events."""
    return (
        spark.read.table("`e-commerce`.silver.silver_events")
        .filter(F.col("product_id").isNotNull())
        .groupBy("product_id")
        .agg(
            F.min("event_time").alias("first_seen"),
            F.max("event_time").alias("last_seen"),
        )
    )
