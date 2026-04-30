from kafka import KafkaConsumer
from flask import Flask, request, jsonify
import time, threading

app = Flask(__name__)
BOOTSTRAP = "kafka:9092"

state = {
    "fault_mode": "healthy"
}

def consume_loop():
    while True:
        try:
            consumer = KafkaConsumer(
                "test-topic",
                bootstrap_servers=BOOTSTRAP,
                group_id="group-class5",
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                max_poll_records=50,
                max_poll_interval_ms=300000 # Keep interval very high to avoid accidental rebalancing (class1)
            )
            print("[Consumer] Connected.")
            break
        except Exception as e:
            print(f"[Consumer] Waiting for Kafka... {e}")
            time.sleep(2)

    while True:
        try:
            messages = consumer.poll(timeout_ms=1000)
            for topic_partition, msgs in messages.items():
                for msg in msgs:
                    # In healthy state, process faster than producer generates.
                    # In fault state, simulate heavy I/O/DB call making processing slower than producer.
                    if state["fault_mode"] == "slow_consumer":
                        time.sleep(0.1) # 10 messages/sec (vs producer's 50 msgs/sec -> building lag rapidly)
                    else:
                        time.sleep(0.005) # 200 messages/sec capacity (easily handles producer)
        except Exception as e:
            print(f"[Consumer] Poll error: {e}")
            time.sleep(1)

@app.route('/setState', methods=['POST'])
def set_state():
    data = request.json
    if "fault_mode" in data:
        state["fault_mode"] = data["fault_mode"]
    print(f"[Consumer API] State updated: {state}")
    return jsonify({"status": "success", "state": state})

if __name__ == "__main__":
    t = threading.Thread(target=consume_loop, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=5000)
