import json
import os
import random
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewPartitions, NewTopic

BOOTSTRAP = "kafka:9092"
TOPIC = "test-topic"
CONTROL_FILE = Path("/app/data/control.json")

DEFAULT_CONFIG = {
    "producer_rate": 10,  # messages per second
    "num_partitions": 6,
    "message_size_bytes": 100,
    "partition_strategy": "hash",  # "hash" or "uniform"
    "consumer_mode": "healthy",
}

_config_cache = DEFAULT_CONFIG.copy()
_last_config_read = 0.0
CONFIG_READ_INTERVAL = 3.0  # seconds


def read_config() -> dict:
    global _config_cache, _last_config_read
    now = time.time()
    if now - _last_config_read < CONFIG_READ_INTERVAL:
        return _config_cache
    try:
        _config_cache = json.loads(CONTROL_FILE.read_text())
        _last_config_read = now
    except Exception:
        pass
    return _config_cache


# ── Connect to Kafka ───────────────────────────────────────────────────────────

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
        print("[Producer] Connected to Kafka.")
        break
    except Exception as e:
        print(f"[Producer] Waiting for Kafka... {e}")
        time.sleep(2)

# ── Ensure topic exists ────────────────────────────────────────────────────────

current_partitions = 6
try:
    admin.create_topics(
        [NewTopic(name=TOPIC, num_partitions=current_partitions, replication_factor=1)]
    )
    print(f"[Producer] Created topic '{TOPIC}' with {current_partitions} partitions.")
except Exception as e:
    print(f"[Producer] Topic may already exist: {e}")

# ── Main loop ──────────────────────────────────────────────────────────────────

i = 0
while True:
    config = read_config()
    desired_partitions = int(config.get("num_partitions", 6))

    # Handle partition count changes
    if desired_partitions != current_partitions:
        if desired_partitions > current_partitions:
            try:
                admin.create_partitions(
                    {TOPIC: NewPartitions(total_count=desired_partitions)}
                )
                print(
                    f"[Producer] Increased partitions: {current_partitions} → {desired_partitions}"
                )
                current_partitions = desired_partitions
            except Exception as e:
                print(f"[Producer] Could not increase partitions: {e}")
        else:
            # Kafka doesn't allow reducing partitions; delete and recreate topic
            try:
                admin.delete_topics([TOPIC])
                time.sleep(3)
                admin.create_topics(
                    [
                        NewTopic(
                            name=TOPIC,
                            num_partitions=desired_partitions,
                            replication_factor=1,
                        )
                    ]
                )
                print(
                    f"[Producer] Recreated topic with {desired_partitions} partitions."
                )
                current_partitions = desired_partitions
            except Exception as e:
                print(f"[Producer] Could not recreate topic: {e}")

    producer_rate = float(config.get("producer_rate", 10))
    message_size = int(config.get("message_size_bytes", 100))
    strategy = config.get("partition_strategy", "hash")

    # Choose partition key
    if strategy == "uniform":
        key = b"same-key"  # All messages land on partition 0 → partition skew
    else:
        key = f"key-{random.randint(0, max(current_partitions - 1, 0))}".encode()

    # Build payload of target size
    header = f"msg-{i}:".encode()
    pad_size = max(message_size - len(header), 0)
    value = header + os.urandom(pad_size)

    try:
        producer.send(TOPIC, key=key, value=value)
    except Exception as e:
        print(f"[Producer] Send error: {e}")

    # Rate control
    if producer_rate > 0:
        time.sleep(1.0 / producer_rate)

    if i % 200 == 0:
        print(
            f"[Producer] msgs={i} | rate={producer_rate:.0f}/s | "
            f"size={message_size}B | strategy={strategy} | partitions={current_partitions}"
        )
    i += 1
