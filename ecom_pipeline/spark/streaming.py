from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as _sum, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType,
)
import pymongo
import os
import pyspark

# Force SPARK_HOME to the pip-installed pyspark, avoiding conflicts with C:\spark
os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)
# Provide Hadoop environment so Windows NativeIO won't throw UnsatisfiedLinkError
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"

KAFKA_BROKER = "localhost:9092"
TOPIC        = "product_events"
MONGO_URI    = "mongodb://localhost:27017/?directConnection=true"
DB_NAME      = "ecom_pipeline"

SCHEMA = StructType([
    StructField("event_id",     IntegerType()),
    StructField("event_type",   StringType()),
    StructField("user_id",      StringType()),
    StructField("product_id",   IntegerType()),
    StructField("title",        StringType()),
    StructField("category",     StringType()),
    StructField("price",        FloatType()),
    StructField("rating",       FloatType()),
    StructField("rating_count", IntegerType()),
    StructField("timestamp",    StringType()),
])


def write_to_mongo(batch_df, batch_id):
    """Foreachbatch handler — called every 5 seconds by Spark."""
    if batch_df.isEmpty():
        return

    client = pymongo.MongoClient(MONGO_URI)
    db     = client[DB_NAME]

    # ── 1. Raw events ─────────────────────────────────────────────
    rows = [r.asDict() for r in batch_df.collect()]
    if rows:
        db["raw_events"].insert_many(rows)

    # ── 2. Category stats — running counters ──────────────────────
    cat_agg = batch_df.groupBy("category").agg(
        count("*").alias("event_count"),
        _sum((col("event_type") == "purchase").cast("int")).alias("purchases"),
        avg("price").alias("avg_price"),
    ).collect()

    for r in cat_agg:
        db["category_stats"].update_one(
            {"category": r["category"]},
            {
                "$inc": {
                    "event_count": int(r["event_count"]),
                    "purchases":   int(r["purchases"]),
                },
                "$set": {"avg_price": float(r["avg_price"])},
            },
            upsert=True,
        )

    # ── 3. Product stats — running counters ───────────────────────
    prod_agg = batch_df.groupBy(
        "product_id", "title", "category", "price"
    ).agg(
        count("*").alias("event_count")
    ).collect()

    for r in prod_agg:
        db["product_stats"].update_one(
            {"product_id": r["product_id"]},
            {
                "$inc": {"event_count": int(r["event_count"])},
                "$set": {
                    "title":    r["title"],
                    "category": r["category"],
                    "price":    float(r["price"]),
                },
            },
            upsert=True,
        )

    client.close()
    print(f"[Spark] Batch {batch_id} — {len(rows)} raw events written to MongoDB")


def main():
    spark = (
        SparkSession.builder
        .appName("EcomPipeline")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
        .select(from_json(col("value").cast("string"), SCHEMA).alias("d"))
        .select("d.*")
        .filter(col("event_type").isNotNull())
    )

    print("[Spark] Streaming started. Waiting for events...")

    query = (
        stream.writeStream
        .foreachBatch(write_to_mongo)
        .outputMode("update")
        .trigger(processingTime="5 seconds")
        .option("checkpointLocation", "/tmp/spark_checkpoint")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
