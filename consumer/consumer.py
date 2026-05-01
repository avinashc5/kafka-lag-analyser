import json
import os
import time
from pathlib import Path

from kafka import KafkaConsumer

BOOTSTRAP = "kafka:9092"
CONTROL_FILE = Path("/app/data/control.json")

# Fall back to env var for compatibility with direct docker-compose env injection
_ENV_MODE = os.environ.get("FAULT_MODE", "healthy")

_mode_cache = _ENV_MODE
_last_mode_read = 0.0
MODE_READ_INTERVAL = 3.0  # seconds


def read_consumer_mode() -> str:
    global _mode_cache, _last_mode_read
    now = time.time()
    if now - _last_mode_read < MODE_READ_INTERVAL:
        return _mode_cache
    try:
        config = json.loads(CONTROL_FILE.read_text())
        _mode_cache = config.get("consumer_mode", _ENV_MODE)
        _last_mode_read = now
    except Exception:
        pass
    return _mode_cache


while True:
    try:
        consumer = KafkaConsumer(
            "test-topic",
            bootstrap_servers=BOOTSTRAP,
            group_id="group1",
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
        )
        print(f"[Consumer] Connected. initial_mode={_ENV_MODE}")
        break
    except Exception as e:
        print(f"[Consumer] Waiting for Kafka... {e}")
        time.sleep(2)

msg_count = 0
for msg in consumer:
    mode = read_consumer_mode()

    if mode == "slow_consumer":
        time.sleep(2)
    elif mode == "rebalance_loop":
        if msg_count > 20:
            print("[Consumer] Simulating crash for rebalance loop")
            raise SystemExit(1)

    if msg_count % 100 == 0:
        print(f"[Consumer] mode={mode} offset={msg.offset} partition={msg.partition}")
    msg_count += 1
