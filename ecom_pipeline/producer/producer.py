import json
import time
import random
import os
import requests
from kafka import KafkaProducer
from datetime import datetime

# ── Config from environment variables ────────────────────────────────────────
KAFKA_BROKER   = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_USERNAME = os.environ.get("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.environ.get("KAFKA_PASSWORD", "")
TOPIC          = "product_events"
API_URL        = "https://fakestoreapi.com/products"


def get_producer():
    """Build a KafkaProducer. Supports plain (local) and SASL_SSL (Upstash)."""
    kwargs = dict(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        retry_backoff_ms=1000,
    )
    # If credentials are present, use SASL_SSL (required by Upstash Kafka)
    if KAFKA_USERNAME and KAFKA_PASSWORD:
        kwargs.update(
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_plain_username=KAFKA_USERNAME,
            sasl_plain_password=KAFKA_PASSWORD,
        )
    return KafkaProducer(**kwargs)


def fetch_products():
    resp = requests.get(API_URL, timeout=10)
    resp.raise_for_status()
    return resp.json()


def build_event(product):
    return {
        "event_id":     random.randint(10000, 99999),
        "event_type":   random.choices(
                            ["view", "click", "purchase"],
                            weights=[6, 3, 1]
                        )[0],
        "user_id":      f"U{random.randint(1, 50):03d}",
        "product_id":   product["id"],
        "title":        product["title"],
        "category":     product["category"],
        "price":        product["price"],
        "rating":       product["rating"]["rate"],
        "rating_count": product["rating"]["count"],
        "timestamp":    datetime.utcnow().isoformat(),
    }


def main():
    print("Connecting to Kafka broker at", KAFKA_BROKER, "...")
    producer = get_producer()

    print("Fetching products from Fake Store API...")
    products = fetch_products()
    print(f"Loaded {len(products)} products. Streaming to Kafka topic '{TOPIC}'...\n")

    while True:
        product = random.choice(products)
        event   = build_event(product)
        producer.send(TOPIC, value=event)
        print(
            f"[{event['timestamp']}] {event['event_type']:8s} | "
            f"User {event['user_id']} | {event['category']:25s} | "
            f"${event['price']:.2f}"
        )
        time.sleep(1)


if __name__ == "__main__":
    main()
