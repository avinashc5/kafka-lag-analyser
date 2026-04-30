from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import time, random

BOOTSTRAP = "kafka:9092"

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)
        print("[Producer] Connected.")
        break
    except Exception as e:
        print(f"[Producer] Waiting for Kafka... {e}")
        time.sleep(2)

try:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    admin.create_topics([NewTopic(name="test-topic", num_partitions=6, replication_factor=1)])
    print("[Producer] Created topic")
except Exception:
    pass

i = 0
while True:
    key = f"key-{random.randint(0, 5)}".encode()
    val = f"msg-{i}".encode()

    try:
        producer.send("test-topic", key=key, value=val)
    except Exception:
        pass

    time.sleep(0.02)  # Emit ~50 messages per second constantly
    i += 1
