import os
import csv
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

INPUT_PATH = "vm1/landing/order_metrics_landed.jsonl"
OUTPUT_DIR = "vm1/output"

SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("restaurant_id", StringType(), True),
    StructField("delivery_partner_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("order_amount", DoubleType(), True),
    StructField("delivery_fee", DoubleType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("preparation_time_min", DoubleType(), True),
    StructField("delivery_time_min", DoubleType(), True),
    StructField("fulfillment_time_min", DoubleType(), True),
    StructField("delivery_speed_kmph", DoubleType(), True),
    StructField("delay_min", DoubleType(), True),
    StructField("delivery_status", StringType(), True),
    StructField("delay_category", StringType(), True),
])


def write_csv(df, path):
    # Data is small: collect and write one plain CSV file with the exact required name
    with open(path, "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(df.columns)
        for row in df.collect():
            w.writerow(list(row))


def main():
    if not os.path.exists(INPUT_PATH):
        print(f"Metrics landing file not found at {INPUT_PATH}.")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    spark = (
        SparkSession.builder.appName("SparkJob2-OperationsAnalytics")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    in_uri = "file://" + os.path.abspath(INPUT_PATH)
    df = spark.read.schema(SCHEMA).option("mode", "DROPMALFORMED").json(in_uri)

    # Exact duplicate orders are counted once
    df = df.dropDuplicates()
    print(f"Loaded {df.count()} unique records for analytics.")

    delayed = F.sum(F.when(F.col("delivery_status") == "DELAYED", 1).otherwise(0))

    # A. Restaurant performance
    restaurant = (
        df.groupBy("restaurant_id")
        .agg(
            F.count("*").alias("order_count"),
            F.round(F.avg("order_amount"), 2).alias("avg_order_amount"),
            F.round(F.avg("preparation_time_min"), 2).alias("avg_preparation_time_min"),
            F.round(F.avg("delivery_time_min"), 2).alias("avg_delivery_time_min"),
            delayed.alias("delayed_order_count"),
        )
        .withColumn("delay_rate_pct", F.round(100.0 * F.col("delayed_order_count") / F.col("order_count"), 2))
        .orderBy("restaurant_id")
    )

    # B. City delivery performance
    city = (
        df.groupBy("city")
        .agg(
            F.count("*").alias("order_count"),
            F.round(F.avg("order_amount"), 2).alias("avg_order_amount"),
            F.round(F.avg("delivery_time_min"), 2).alias("avg_delivery_time_min"),
            delayed.alias("delayed_order_count"),
        )
        .withColumn("delay_rate_pct", F.round(100.0 * F.col("delayed_order_count") / F.col("order_count"), 2))
        .orderBy("city")
    )

    rest_csv = os.path.join(OUTPUT_DIR, "restaurant_summary.csv")
    city_csv = os.path.join(OUTPUT_DIR, "city_summary.csv")
    write_csv(restaurant, rest_csv)
    write_csv(city, city_csv)
    print(f"Wrote {rest_csv} and {city_csv}.")
    spark.stop()


if __name__ == "__main__":
    main()