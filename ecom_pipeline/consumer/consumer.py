import json
import os
import time
import threading
import pymongo
from confluent_kafka import Consumer, KafkaError
from http.server import HTTPServer, BaseHTTPRequestHandler

KAFKA_BROKER   = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_USERNAME = os.environ.get("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.environ.get("KAFKA_PASSWORD", "")
MONGO_URI      = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
TOPIC          = "product_events"
DB_NAME        = "ecom_pipeline"

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

def get_consumer():
    conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "ecom-consumer-group",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    }
    if KAFKA_USERNAME and KAFKA_PASSWORD:
        conf.update({
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "SCRAM-SHA-256",
            "sasl.username": KAFKA_USERNAME,
            "sasl.password": KAFKA_PASSWORD,
        })
    c = Consumer(conf)
    c.subscribe([TOPIC])
    return c

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

def main():
    threading.Thread(target=start_health_server, daemon=True).start()
    print("[Consumer] Health server started on port 8080")

    print("[Consumer] Connecting to MongoDB...")
    mongo_client = pymongo.MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    print("[Consumer] MongoDB connected.")

    while True:
        try:
            print("[Consumer] Connecting to Kafka broker:", KAFKA_BROKER)
            consumer = get_consumer()
            print(f"[Consumer] Subscribed to '{TOPIC}'. Waiting for events...")

            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise Exception(msg.error())

                event = json.loads(msg.value().decode("utf-8"))
                try:
                    process_event(db, event)
                    print(
                        f"[Consumer] {event.get('event_type','?'):8s} | "
                        f"User {event.get('user_id','?')} | "
                        f"{str(event.get('category','?'))[:25]:25s} | "
                        f"${float(event.get('price', 0)):.2f}"
                    )
                except Exception as exc:
                    print(f"[Consumer] Error processing: {exc}")

        except Exception as e:
            print(f"[Consumer] Kafka error: {e}")
            print("[Consumer] Retrying in 10 seconds...")
            time.sleep(10)

if __name__ == "__main__":
    main()
