from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


@dp.materialized_view(
    name="`e-commerce`.gold.dim_event_types",
    comment="Event type dimension - unique event types",
)
def dim_event_types():
    """Build event type dimension from silver events."""
    return (
        spark.read.table("`e-commerce`.silver.silver_events")
        .filter(F.col("event_type").isNotNull())
        .groupBy("event_type")
        .agg(F.count("*").alias("total_events"))
    )
