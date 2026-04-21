import json
import time
import random
import os
import requests
import threading
from confluent_kafka import Producer
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKER   = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_USERNAME = os.environ.get("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.environ.get("KAFKA_PASSWORD", "")
TOPIC          = "product_events"
API_URL        = "https://dummyjson.com/products?limit=100"

# ── Health check server ───────────────────────────────────────────────────────
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

# ── Kafka Producer ────────────────────────────────────────────────────────────
def get_producer():
    conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "socket.timeout.ms": 60000,
        "message.timeout.ms": 60000,
    }
    if KAFKA_USERNAME and KAFKA_PASSWORD:
        conf.update({
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "SCRAM-SHA-256",
            "sasl.username": KAFKA_USERNAME,
            "sasl.password": KAFKA_PASSWORD,
        })
    return Producer(conf)

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
    threading.Thread(target=start_health_server, daemon=True).start()
    print("[Producer] Health server started on port 8081")

    print("[Producer] Fetching products...")
    products = fetch_products()
    print(f"[Producer] Loaded {len(products)} products.")

    while True:
        try:
            print(f"[Producer] Connecting to Kafka: {KAFKA_BROKER}")
            producer = get_producer()
            print(f"[Producer] Connected! Streaming to '{TOPIC}'...")

            while True:
                product = random.choice(products)
                event   = build_event(product)
                producer.produce(
                    TOPIC,
                    value=json.dumps(event).encode("utf-8")
                )
                producer.poll(0)
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
