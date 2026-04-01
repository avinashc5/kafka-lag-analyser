from kafka import KafkaProducer
import time

while True:
    try:
        producer = KafkaProducer(bootstrap_servers='kafka:9092')
        print("Connected to Kafka")
        break
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        print("Waiting for Kafka to start...")
        time.sleep(2)

i = 0
while True:
    producer.send('test-topic', f"msg-{i}".encode())
    print(f"Sent {i}")
    i += 1
    time.sleep(0.5)