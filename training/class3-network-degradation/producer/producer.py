from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import time, random

BOOTSTRAP = "kafka:9092,kafka2:9092"

while True:
    try:
        # Require acks from both the leader AND the follower (broker 2)
        producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, acks="all")
        print("[Producer] Connected.")
        break
    except Exception as e:
        print(f"[Producer] Waiting for Kafka... {e}")
        time.sleep(2)

try:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    admin.create_topics([NewTopic(name="test-topic", num_partitions=6, replication_factor=2)])
    print("[Producer] Created topic")
except Exception:
    pass

i = 0
while True:
    key = f"key-{random.randint(0, 5)}".encode()
    try:
        producer.send("test-topic", key=key, value=f"msg-{i}".encode())
    except Exception:
        pass
    
    time.sleep(0.05)
    i += 1