from kafka import KafkaConsumer
import time

BOOTSTRAP = "kafka:9092"

while True:
    try:
        consumer = KafkaConsumer(
            "test-topic",
            bootstrap_servers=BOOTSTRAP,
            group_id="group-class2",
            enable_auto_commit=True,
            auto_commit_interval_ms=1000
        )
        print("[Consumer] Connected.")
        break
    except Exception as e:
        print(f"[Consumer] Waiting for Kafka... {e}")
        time.sleep(2)

print("[Consumer] Started consuming purely to consume.")
for msg in consumer:
    # Just read continuously so we can track lag if the broker bogs down
    time.sleep(0.005)
