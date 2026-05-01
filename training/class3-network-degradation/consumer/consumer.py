from kafka import KafkaConsumer
import time

BOOTSTRAP = "kafka:9092"

while True:
    try:
        consumer = KafkaConsumer(
            "test-topic",
            bootstrap_servers=BOOTSTRAP,
            group_id="group-class3",
            enable_auto_commit=True,
            auto_commit_interval_ms=1000
        )
        print("[Consumer] Connected.")
        break
    except Exception as e:
        print(f"[Consumer] Waiting for Kafka... {e}")
        time.sleep(2)

for msg in consumer:
    time.sleep(0.005)