import json
import time
import random
import requests
from kafka import KafkaProducer
from datetime import datetime

KAFKA_BROKER = "localhost:9092"
TOPIC        = "product_events"
API_URL      = "https://fakestoreapi.com/products"


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
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        retry_backoff_ms=1000,
    )

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
