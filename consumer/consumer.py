from kafka import KafkaConsumer
import time

while True:
    try:
        consumer = KafkaConsumer(
            'test-topic',
            bootstrap_servers='kafka:9092',
            group_id='group1'
        )
        print("Connected to Kafka")
        break
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        print("Waiting for Kafka to start...")
        time.sleep(2)

for msg in consumer:
    print(msg.value)
    time.sleep(2)  # slow consumer → creates lag