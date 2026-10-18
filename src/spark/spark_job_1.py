import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType

INPUT_PATH = "vm4/landing/orders_landed.jsonl"
OUTPUT_PATH = "vm4/output/order_metrics.jsonl"

TS_FMT = "yyyy-MM-dd HH:mm:ss"

FIELDS = [
    "order_id", "customer_id", "restaurant_id", "delivery_partner_id", "city",
    "order_timestamp", "accepted_timestamp", "picked_up_timestamp",
    "expected_delivery_timestamp", "delivered_timestamp",
    "distance_km", "order_amount", "delivery_fee", "status",
]
TS_FIELDS = [
    "order_timestamp", "accepted_timestamp", "picked_up_timestamp",
    "expected_delivery_timestamp", "delivered_timestamp",
]


def main():
    if not os.path.exists(INPUT_PATH):
        print(f"Landing file not found at {INPUT_PATH}.")
        return

    spark = (
        SparkSession.builder.appName("SparkJob1-OrderEnrichment")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")  # invalid casts/timestamps -> null, not an error
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Local filesystem only (no HDFS)
    in_uri = "file://" + os.path.abspath(INPUT_PATH)

    # Every field is read as a string first so that bad types can be detected explicitly.
    schema = StructType([StructField(f, StringType(), True) for f in FIELDS])

    raw = spark.read.text(in_uri)
    # Malformed JSON (or a non-object JSON value) -> NULL struct
    parsed = raw.select(F.from_json(F.col("value"), schema).alias("r")).where(F.col("r").isNotNull())
    df = parsed.select([F.col("r." + f).alias(f) for f in FIELDS])
    print(f"Total JSON records parsed: {df.count()}")

    # 1) A required field is missing / null / empty
    for f in FIELDS:
        df = df.where(F.col(f).isNotNull() & (F.trim(F.col(f)) != ""))

    # 2) Strict timestamp parsing; invalid timestamp -> record dropped
    for f in TS_FIELDS:
        df = df.withColumn("_" + f, F.to_timestamp(F.col(f), TS_FMT))
        df = df.where(F.col("_" + f).isNotNull())

    # 3) Numeric fields must be valid numbers
    for f in ["distance_km", "order_amount", "delivery_fee"]:
        df = df.withColumn("_" + f, F.col(f).cast("double"))
        df = df.where(F.col("_" + f).isNotNull() & ~F.isnan(F.col("_" + f)))

    # 4) Business cleaning rules
    df = df.where(
        (F.col("_distance_km") > 0)
        & (F.col("_order_amount") > 0)
        & (F.col("status") == "DELIVERED")
    )

    # 5) Exact duplicate orders are counted once (all 14 original fields identical)
    df = df.dropDuplicates(FIELDS)

    def minutes(later, earlier):
        return (F.col("_" + later).cast("long") - F.col("_" + earlier).cast("long")) / 60.0

    df = (
        df.withColumn("_prep", minutes("picked_up_timestamp", "accepted_timestamp"))
        .withColumn("_deliv", minutes("delivered_timestamp", "picked_up_timestamp"))
        .withColumn("_fulfil", minutes("delivered_timestamp", "order_timestamp"))
        .withColumn("_delay", F.greatest(F.lit(0.0), minutes("delivered_timestamp", "expected_delivery_timestamp")))
    )

    out = df.select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("restaurant_id"),
        F.col("delivery_partner_id"),
        F.col("city"),
        F.col("_order_amount").alias("order_amount"),
        F.col("_delivery_fee").alias("delivery_fee"),
        F.col("_distance_km").alias("distance_km"),
        F.round("_prep", 2).alias("preparation_time_min"),
        F.round("_deliv", 2).alias("delivery_time_min"),
        F.round("_fulfil", 2).alias("fulfillment_time_min"),
        F.round(F.col("_distance_km") / (F.col("_deliv") / 60.0), 2).alias("delivery_speed_kmph"),
        F.round("_delay", 2).alias("delay_min"),
        F.when(F.col("_delay") == 0, "ON_TIME").otherwise("DELAYED").alias("delivery_status"),
        F.when(F.col("_delay") == 0, "ON_TIME")
        .when(F.col("_delay") <= 15, "SLIGHT_DELAY")
        .when(F.col("_delay") <= 30, "MODERATE_DELAY")
        .otherwise("SEVERE_DELAY")
        .alias("delay_category"),
    ).orderBy("order_id")

    lines = [r["j"] for r in out.select(F.to_json(F.struct(*out.columns)).alias("j")).collect()]
    print(f"Valid enriched records produced: {len(lines)}")

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        for line in lines:
            f.write(line + "\n")

    print(f"Successfully wrote metrics to {OUTPUT_PATH}.")
    spark.stop()


if __name__ == "__main__":
    main()