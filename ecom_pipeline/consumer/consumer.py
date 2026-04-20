"""
Lightweight Kafka → MongoDB consumer.
Replaces the PySpark streaming job for cloud deployment.
Reads from the 'product_events' Kafka topic and writes aggregated
stats to MongoDB — same collections that the Node.js server reads.
"""

import json
import os
import pymongo
from kafka import KafkaConsumer

# ── Config from environment variables ────────────────────────────────────────
KAFKA_BROKER   = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_USERNAME = os.environ.get("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.environ.get("KAFKA_PASSWORD", "")
MONGO_URI      = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
TOPIC          = "product_events"
DB_NAME        = "ecom_pipeline"


def get_consumer():
    """Build a KafkaConsumer. Supports plain (local) and SASL_SSL (Upstash)."""
    kwargs = dict(
        bootstrap_servers=KAFKA_BROKER,
        group_id="ecom-consumer-group",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    if KAFKA_USERNAME and KAFKA_PASSWORD:
        kwargs.update(
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_plain_username=KAFKA_USERNAME,
            sasl_plain_password=KAFKA_PASSWORD,
        )
    return KafkaConsumer(TOPIC, **kwargs)


def process_event(db, event):
    """Write one event into the three MongoDB collections."""

    # 1. Raw events (keep last 500 only to avoid unbounded growth)
    db["raw_events"].insert_one({k: v for k, v in event.items() if k != "_id"})
    db["raw_events"].delete_many(
        {"_id": {"$lt": db["raw_events"].find_one(
            sort=[("_id", pymongo.DESCENDING)]
        )["_id"]}}
    ) if db["raw_events"].count_documents({}) > 500 else None

    # 2. Category stats — running counters
    db["category_stats"].update_one(
        {"category": event["category"]},
        {
            "$inc": {
                "event_count": 1,
                "purchases":   1 if event["event_type"] == "purchase" else 0,
            },
            "$set": {"avg_price": float(event["price"])},
        },
        upsert=True,
    )

    # 3. Product stats — running counters
    db["product_stats"].update_one(
        {"product_id": event["product_id"]},
        {
            "$inc": {"event_count": 1},
            "$set": {
                "title":    event["title"],
                "category": event["category"],
                "price":    float(event["price"]),
            },
        },
        upsert=True,
    )


def main():
    print("[Consumer] Connecting to MongoDB...")
    mongo_client = pymongo.MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    print("[Consumer] MongoDB connected.")

    print("[Consumer] Connecting to Kafka broker:", KAFKA_BROKER)
    consumer = get_consumer()
    print(f"[Consumer] Subscribed to topic '{TOPIC}'. Waiting for events...")

    for message in consumer:
        event = message.value
        try:
            process_event(db, event)
            print(
                f"[Consumer] {event.get('event_type', '?'):8s} | "
                f"User {event.get('user_id', '?')} | "
                f"{str(event.get('category', '?'))[:25]:25s} | "
                f"${float(event.get('price', 0)):.2f}"
            )
        except Exception as exc:
            print(f"[Consumer] Error processing event: {exc}")


if __name__ == "__main__":
    main()
