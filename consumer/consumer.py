from kafka import KafkaConsumer
import time

while True:
    try:
        consumer = KafkaConsumer(
            "test-topic",
            bootstrap_servers="kafka:9092",
            group_id="group1",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        print("Connected to Kafka")
        break
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        print("Waiting for Kafka to start...")
        time.sleep(2)

for msg in consumer:
    print(msg.value)
    consumer.commit()
    time.sleep(0.5)  # slow consumer → creates lag