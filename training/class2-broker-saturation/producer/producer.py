from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from flask import Flask, request, jsonify
import time, threading, random
import os

app = Flask(__name__)
BOOTSTRAP = "kafka:9092"

state = {
    "fault_mode": "healthy",
    "payload_size": 100
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
        key = f"key-{random.randint(0, 5)}".encode()

        size = state["payload_size"]
        if size > 1000:
            val = os.urandom(size) # Generate massive payload to saturate disk
        else:
            val = f"msg-{i}".zfill(size).encode()

        try:
            producer.send("test-topic", key=key, value=val)
        except Exception as e:
            print(f"[Producer] Send error: {e}")

        if state["fault_mode"] == "disk_io_saturation":
            time.sleep(0.05) # Slightly slower to avoid pure memory flood, but frequent enough for 5MB chunks to overwhelm storage
        else:
            time.sleep(0.1)

        i += 1

@app.route('/setState', methods=['POST'])
def set_state():
    data = request.json
    if "fault_mode" in data:
        state["fault_mode"] = data["fault_mode"]
        if state["fault_mode"] == "disk_io_saturation":
            state["payload_size"] = 5 * 1024 * 1024  # 5MB
        else:
            state["payload_size"] = 100
    print(f"[Producer API] State updated: {state}")
    return jsonify({"status": "success", "state": state})

if __name__ == "__main__":
    t = threading.Thread(target=produce_loop, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=5000)
