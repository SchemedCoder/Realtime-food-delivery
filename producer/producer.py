from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open("../data/orders.json") as f:

    for line in f:

        data = json.loads(line)

        producer.send("food_orders", value=data)

        print("Sent:", data)

        time.sleep(2)
