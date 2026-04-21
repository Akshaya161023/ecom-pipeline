"""
Lightweight Kafka → MongoDB consumer.
Replaces the PySpark streaming job for cloud deployment.
"""
import json
import os
import time
import threading
import pymongo
from kafka import KafkaConsumer
from http.server import HTTPServer, BaseHTTPRequestHandler

# ── Config from environment variables ────────────────────────────────────────
KAFKA_BROKER   = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_USERNAME = os.environ.get("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.environ.get("KAFKA_PASSWORD", "")
MONGO_URI      = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
TOPIC          = "product_events"
DB_NAME        = "ecom_pipeline"

# ── Health check server (required by Render) ─────────────────────────────────
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, *args):
        pass

def start_health_server():
    server = HTTPServer(("0.0.0.0", 8080), HealthHandler)
    server.serve_forever()

# ── Kafka Consumer setup ──────────────────────────────────────────────────────
def get_consumer():
    kwargs = dict(
        bootstrap_servers=KAFKA_BROKER,
        group_id="ecom-consumer-group",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=60000,
    )
    if KAFKA_USERNAME and KAFKA_PASSWORD:
        kwargs.update(
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_plain_username=KAFKA_USERNAME,
            sasl_plain_password=KAFKA_PASSWORD,
        )
    return KafkaConsumer(TOPIC, **kwargs)

# ── Process Event ─────────────────────────────────────────────────────────────
def process_event(db, event):
    # 1. Raw events (keep last 500)
    db["raw_events"].insert_one({k: v for k, v in event.items() if k != "_id"})
    if db["raw_events"].count_documents({}) > 500:
        oldest = db["raw_events"].find_one(sort=[("_id", pymongo.ASCENDING)])
        if oldest:
            db["raw_events"].delete_one({"_id": oldest["_id"]})

    # 2. Category stats
    db["category_stats"].update_one(
        {"category": event["category"]},
        {
            "$inc": {
                "event_count": 1,
                "purchases": 1 if event["event_type"] == "purchase" else 0,
            },
            "$set": {"avg_price": float(event["price"])},
        },
        upsert=True,
    )

    # 3. Product stats
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

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    # Start health server FIRST
    threading.Thread(target=start_health_server, daemon=True).start()
    print("[Consumer] Health server started on port 8080")

    # Connect to MongoDB
    print("[Consumer] Connecting to MongoDB...")
    mongo_client = pymongo.MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    print("[Consumer] MongoDB connected.")

    # Connect to Kafka with retry
    while True:
        try:
            print("[Consumer] Connecting to Kafka broker:", KAFKA_BROKER)
            consumer = get_consumer()
            print(f"[Consumer] Subscribed to topic '{TOPIC}'. Waiting for events...")

            for message in consumer:
                event = message.value
                try:
                    process_event(db, event)
                    print(
                        f"[Consumer] {event.get('event_type','?'):8s} | "
                        f"User {event.get('user_id','?')} | "
                        f"{str(event.get('category','?'))[:25]:25s} | "
                        f"${float(event.get('price', 0)):.2f}"
                    )
                except Exception as exc:
                    print(f"[Consumer] Error processing event: {exc}")

        except Exception as e:
            print(f"[Consumer] Kafka error: {e}")
            print("[Consumer] Retrying in 10 seconds...")
            time.sleep(10)

if __name__ == "__main__":
    main()
