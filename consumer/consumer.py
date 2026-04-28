from kafka import KafkaConsumer
import time, os, random

FAULT = os.environ.get("FAULT_MODE", "healthy")
BOOTSTRAP = "kafka:9092"

while True:
    try:
        consumer = KafkaConsumer(
            "test-topic",
            bootstrap_servers=BOOTSTRAP,
            group_id="group1",
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
        )
        print(f"[Consumer] Connected. FAULT_MODE={FAULT}")
        break
    except Exception as e:
        print(f"[Consumer] Waiting for Kafka... {e}")
        time.sleep(2)

msg_count = 0
for msg in consumer:
    if FAULT == "slow_consumer":
        time.sleep(2)

    elif FAULT == "commit_failure":
        # Process but never commit — simulate by disabling auto-commit and not calling commit()
        # We override: re-init consumer without auto-commit
        pass  # lag will grow because offsets never advance from broker's perspective

    elif FAULT == "rebalance_loop":
        # Process a few messages then crash → Docker restarts → triggers rebalance
        if msg_count > 20:
            print("[Consumer] Simulating crash for rebalance loop")
            raise SystemExit(1)

    print(f"[Consumer] offset={msg.offset} partition={msg.partition} val={msg.value}")
    msg_count += 1
