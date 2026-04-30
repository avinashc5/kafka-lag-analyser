from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from flask import Flask, request, jsonify
import time, threading, random

app = Flask(__name__)
BOOTSTRAP = "kafka:9092"

state = {
    "fault_mode": "healthy"
}

def produce_loop():
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
        if state["fault_mode"] == "partition_skew":
            # Force all messages to exactly one partition by using a static key
            key = b"skewed-key"
        else:
            # Spread across partitions randomly
            key = f"key-{random.randint(0, 5)}".encode()

        val = f"msg-{i}".zfill(100).encode()

        try:
            producer.send("test-topic", key=key, value=val)
        except Exception as e:
            pass

        time.sleep(0.01)  # Faster rate to quickly build up lag/size skew
        i += 1

@app.route('/setState', methods=['POST'])
def set_state():
    data = request.json
    if "fault_mode" in data:
        state["fault_mode"] = data["fault_mode"]
    print(f"[Producer API] State updated: {state}")
    return jsonify({"status": "success", "state": state})

if __name__ == "__main__":
    t = threading.Thread(target=produce_loop, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=5000)
