import dlt
from pyspark.sql.functions import avg, col, to_date

# ==============================================================================
# ENTERPRISE declarative MEDALLION ARCHITECTURE PIPELINE
# ==============================================================================

SOURCE_PATH_CONF = "source_path"
DEFAULT_SOURCE_PATH = "/Volumes/enterprise_dev/raw_landing/streaming_source"


# 1. BRONZE LAYER: Real-time Ingestion via Auto Loader
@dlt.table(
    name="bronze_taxi_raw",
    comment="Raw streaming ingestion layer listening to the managed volume landing zone",
)
def bronze_taxi_raw():
    source_path = spark.conf.get(SOURCE_PATH_CONF, DEFAULT_SOURCE_PATH)

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(source_path)
    )


# 2. SILVER LAYER: Cleaning, Quality Controls & Liquid Clustering
@dlt.table(
    name="silver_taxi_cleaned",
    comment="Cleaned data layer utilizing strict expectations and liquid clustering",
)
@dlt.expect_or_drop("valid_distance", "trip_distance > 0")
@dlt.expect_or_drop("valid_fare", "fare_amount > 0")
def silver_taxi_cleaned():
    return (
        dlt.readStream("bronze_taxi_raw")
        .filter(col("passenger_count") > 0)
        .withColumn("trip_date", to_date(col("tpep_pickup_datetime")))
    )


# 3. GOLD LAYER: Aggregated Production Business Metrics
@dlt.table(
    name="gold_taxi_metrics",
    comment="Final analytics aggregated reporting layer",
)
def gold_taxi_metrics():
    return (
        dlt.readStream("silver_taxi_cleaned")
        .groupBy("pickup_zip", "trip_date")
        .agg(
            avg("fare_amount").alias("avg_fare"),
            avg("trip_distance").alias("avg_distance"),
        )
    )
