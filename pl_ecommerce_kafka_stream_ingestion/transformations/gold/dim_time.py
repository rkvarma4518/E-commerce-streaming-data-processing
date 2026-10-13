from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


@dp.materialized_view(
    name="`e-commerce`.gold.dim_time",
    comment="Time dimension - date-level granularity",
)
def dim_time():
    """Build time dimension from silver event dates."""
    return (
        spark.read.table("`e-commerce`.silver.silver_events")
        .filter(F.col("event_time").isNotNull())
        .withColumn("event_date", F.to_date(F.col("event_time")))
        .groupBy("event_date")
        .agg(F.count("*").alias("daily_event_count"))
        .withColumn("date_key", F.date_format(F.col("event_date"), "yyyyMMdd").cast("int"))
        .withColumn("year", F.year(F.col("event_date")))
        .withColumn("month", F.month(F.col("event_date")))
        .withColumn("day", F.dayofmonth(F.col("event_date")))
        .withColumn("day_of_week", F.dayofweek(F.col("event_date")))
        .select("date_key", "event_date", "year", "month", "day", "day_of_week", "daily_event_count")
    )
