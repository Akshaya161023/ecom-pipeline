import json
import time
import os
import pymongo
import threading
from kafka import KafkaConsumer
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler

# ── Config from environment variables ────────────────────────────────────────
KAFKA_BROKER   = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_USERNAME = os.environ.get("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.environ.get("KAFKA_PASSWORD", "")
MONGO_URI      = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
TOPIC          = "product_events"
GROUP_ID       = "ecom-consumer-group"

# ── Health check server (required by Render web services) ────────────────────
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
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
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

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    # Start health server in background thread
    threading.Thread(target=start_health_server, daemon=True).start()
    print("[Consumer] Health server started on port 8080")

    # Connect to MongoDB
    print("[Consumer] Connecting to MongoDB...")
    mongo_client = pymongo.MongoClient(MONGO_URI)
    db = mongo_client["ecom_pipeline"]
    collection = db["events"]
    print("[Consumer] MongoDB connected.")

    # Connect to Kafka
    while True:
        try:
            print(f"[Consumer] Connecting to Kafka broker: {KAFKA_BROKER}")
            consumer = get_consumer()
            print(f"[Consumer] Subscribed to topic '{TOPIC}'. Waiting for events...")

            for message in consumer:
                event = message.value
                event["consumed_at"] = datetime.utcnow().isoformat()
                collection.insert_one(event)
                print(
                    f"[Consumer] Saved → {event.get('event_type','?'):8s} | "
                    f"User {event.get('user_id','?')} | "
                    f"{event.get('category','?'):25s} | "
                    f"${event.get('price', 0):.2f}"
                )

        except Exception as e:
            print(f"[Consumer] Error: {e}")
            print("[Consumer] Retrying in 10 seconds...")
            time.sleep(10)

if __name__ == "__main__":
    main()
