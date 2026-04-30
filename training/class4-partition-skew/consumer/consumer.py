from kafka import KafkaConsumer
import time

BOOTSTRAP = "kafka:9092"

while True:
    try:
        consumer = KafkaConsumer(
            "test-topic",
            bootstrap_servers=BOOTSTRAP,
            group_id="group-class4",
            enable_auto_commit=True,
            auto_commit_interval_ms=1000
        )
        print("[Consumer] Connected.")
        break
    except Exception as e:
        print(f"[Consumer] Waiting for Kafka... {e}")
        time.sleep(2)

print("[Consumer] Started consuming.")
for msg in consumer:
    # Slightly slow consumer to allow lag skew to visibly build on the hot partition
    time.sleep(0.015)
