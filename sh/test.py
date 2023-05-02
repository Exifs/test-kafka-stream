import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import random

# configure Kafka producer
producer = KafkaProducer(bootstrap_servers='rpi4:19092',
                         value_serializer=lambda x:
                         json.dumps(x).encode('utf-8'))

# generate 1000 random JSONs and send them to Kafka with a 1 ms delay between each call
for i in range(100000):
    product_id = random.randint(1, 4)
    user_id = random.randint(0, 200)
    quantity = random.randint(1, 100)
    if random.random() < 0.5:
        type = "INCREASE"
    else:
        type = "DECREASE"
    json_dict = {
        "id": i,
        "product_id": product_id,
        "user_id": user_id,
        "quantity": quantity,
        "type": type
    }

    # send message to Kafka and wait for confirmation
    future = producer.send('stocks-events', value=json_dict)
    try:
        record_metadata = future.get(timeout=10)
    except KafkaError as e:
        print(f"Failed to send message: {e}")
        # handle the error here, for example retry or save the message to a file
    else:
        print(f"Message sent successfully to topic {record_metadata.topic} at offset {record_metadata.offset}")

    # flush the buffer every 10 messages
    if i % 100 == 0:
        producer.flush()

# flush any remaining messages
producer.flush()
