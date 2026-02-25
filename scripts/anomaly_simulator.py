import json, time
from confluent_kafka import Producer

p = Producer({"bootstrap.servers": "localhost:9092"})

for i in range(200):
    p.produce("ecommerce-events", value=json.dumps(
        {"user_id": 1, "event": "click", "product_id": i % 20, "timestamp": time.time()}
    ).encode())
    p.poll(0)
    time.sleep(0.01)

p.flush()
print("[SIMULATOR] Done. user_id=1 should trigger anomaly.")