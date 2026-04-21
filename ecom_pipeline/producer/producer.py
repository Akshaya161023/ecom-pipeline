import json
import time
import random
import os
import requests
from kafka import KafkaProducer
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

# ── Config from environment variables ────────────────────────────────────────
KAFKA_BROKER   = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_USERNAME = os.environ.get("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.environ.get("KAFKA_PASSWORD", "")
TOPIC          = "product_events"
API_URL        = "https://dummyjson.com/products?limit=100"

# ── Health check server (required by Render web services) ────────────────────
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, *args):
        pass

def start_health_server():
    server = HTTPServer(("0.0.0.0", 8081), HealthHandler)
    server.serve_forever()

# ── Kafka Producer setup ──────────────────────────────────────────────────────
def get_producer():
    kwargs = dict(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        retry_backoff_ms=1000,
        request_timeout_ms=30000,
        max_block_ms=30000,
    )
    if KAFKA_USERNAME and KAFKA_PASSWORD:
        kwargs.update(
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_plain_username=KAFKA_USERNAME,
            sasl_plain_password=KAFKA_PASSWORD,
        )
    return KafkaProducer(**kwargs)

# ── Fetch Products ────────────────────────────────────────────────────────────
def fetch_products():
    resp = requests.get(API_URL, timeout=10)
    resp.raise_for_status()
    return resp.json()["products"]

# ── Build Event ───────────────────────────────────────────────────────────────
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
        "rating":       product.get("rating", 0),
        "rating_count": product.get("stock", 0),
        "timestamp":    datetime.utcnow().isoformat(),
    }

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    # Start health server in background thread
    threading.Thread(target=start_health_server, daemon=True).start()
    print("[Producer] Health server started on port 8081")

    # Fetch products once
    print("[Producer] Fetching products...")
    products = fetch_products()
    print(f"[Producer] Loaded {len(products)} products.")

    while True:
        try:
            print(f"[Producer] Connecting to Kafka broker: {KAFKA_BROKER}")
            producer = get_producer()
            print(f"[Producer] Connected! Streaming to topic '{TOPIC}'...\n")

            while True:
                product = random.choice(products)
                event   = build_event(product)
                producer.send(TOPIC, value=event)
                producer.flush()
                print(
                    f"[{event['timestamp']}] {event['event_type']:8s} | "
                    f"User {event['user_id']} | "
                    f"{event['category']:25s} | "
                    f"${event['price']:.2f}"
                )
                time.sleep(2)

        except Exception as e:
            print(f"[Producer] Error: {e}")
            print("[Producer] Retrying in 10 seconds...")
            time.sleep(10)

if __name__ == "__main__":
    main()
