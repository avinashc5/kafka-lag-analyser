from kafka import KafkaConsumer
from flask import Flask, request, jsonify
import time, threading

app = Flask(__name__)
BOOTSTRAP = "kafka:9092"

state = {
    "fault_mode": "healthy",
    "reconnect": False
}

def consume_loop():
    consumer = None

    def connect_kafka():
        while True:
            try:
                c = KafkaConsumer(
                    "test-topic",
                    bootstrap_servers=BOOTSTRAP,
                    group_id="group1",
                    enable_auto_commit=True,
                    auto_commit_interval_ms=1000,
                    session_timeout_ms=10000,
                    max_poll_interval_ms=10000,
                    auto_offset_reset="earliest"
                )
                print("[Consumer] Connected.")
                return c
            except Exception as e:
                print(f"[Consumer] Waiting for Kafka... {e}")
                time.sleep(2)

    consumer = connect_kafka()

    while True:
        try:
            if state["reconnect"]:
                print("[Consumer] Executing explicit disconnect and reconnect!")
                consumer.close()
                time.sleep(2)
                consumer = connect_kafka()
                state["reconnect"] = False

            if state["fault_mode"] == "consumer_timeout":
                time.sleep(12)

            messages = consumer.poll(timeout_ms=1000)
            for topic_partition, msgs in messages.items():
                for msg in msgs:
                    print(f"[Consumer] Consumed msg={msg.value} from partition={msg.partition}")
                    time.sleep(0.01)

        except Exception as e:
            print(f"[Consumer] Error in loop: {e}")
            time.sleep(1)

@app.route('/setState', methods=['POST'])
def set_state():
    data = request.json
    if "fault_mode" in data:
        state["fault_mode"] = data["fault_mode"]
    if "reconnect" in data:
        state["reconnect"] = data["reconnect"]
    print(f"[Consumer API] State updated: {state}")
    return jsonify({"status": "success", "state": state})

if __name__ == "__main__":
    t = threading.Thread(target=consume_loop, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=5000)
