import json, time, random
from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092"})

while True:
    data = {"user_id": random.randint(1, 100), "event": "click", "product_id": random.randint(1, 500), "timestamp": time.time()}
    producer.produce("ecommerce-events", value=json.dumps(data).encode())
    producer.poll(0)  # trigger delivery callbacks, prevents buffer buildup
    time.sleep(0.1)