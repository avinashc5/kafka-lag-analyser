from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import time, os, random

FAULT = os.environ.get("FAULT_MODE", "healthy")
BOOTSTRAP = "kafka:9092"

# Wait for Kafka
while True:
    try:
        producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)
        print(f"[Producer] Connected. FAULT_MODE={FAULT}")
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
    if FAULT == "partition_imbalance":
        # All messages go to partition 0 via fixed key
        key = b"same-key"
    else:
        # Spread across partitions
        key = f"key-{random.randint(0, 5)}".encode()

    producer.send("test-topic", key=key, value=f"msg-{i}".encode())

    if FAULT == "high_throughput":
        # Flood messages to create lag
        pass  # no sleep
    else:
        time.sleep(0.1)

    if i % 100 == 0:
        print(f"[Producer] Sent {i} messages")
    i += 1
