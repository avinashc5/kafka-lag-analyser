from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import time, random

BOOTSTRAP = "kafka:9092"

# Wait for Kafka
while True:
    try:
        producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)
        print("[Producer] Connected.")
        break
    except Exception as e:
        print(f"[Producer] Waiting for Kafka... {e}")
        time.sleep(2)

# Ensure topic exists with multiple partitions
try:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    admin.create_topics(
        [NewTopic(name="test-topic", num_partitions=6, replication_factor=1)]
    )
    print("[Producer] Created topic with 6 partitions")
except Exception as e:
    print(f"[Producer] Topic may already exist: {e}")

i = 0
while True:
    # Spread across partitions randomly
    key = f"key-{random.randint(0, 5)}".encode()

    try:
        producer.send("test-topic", key=key, value=f"msg-{i}".encode())
    except Exception as e:
        print(f"[Producer] Send error: {e}")
        time.sleep(1)

    time.sleep(0.1)

    if i % 100 == 0:
        print(f"[Producer] Sent {i} messages")
    i += 1
