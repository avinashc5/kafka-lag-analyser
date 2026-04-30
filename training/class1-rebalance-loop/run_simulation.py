import subprocess
import time
import csv
import requests
import os
import sqlite3
from datetime import datetime, timezone

CONSUMERS = [
    "http://localhost:5001/setState",
    "http://localhost:5002/setState",
    "http://localhost:5003/setState",
    "http://localhost:5004/setState"
]

def utc_now():
    return datetime.now(timezone.utc).isoformat()

def set_consumers_state(fault_mode="healthy", reconnect=False, target_consumers=CONSUMERS):
    for endpoint in target_consumers:
        try:
            requests.post(endpoint, json={"fault_mode": fault_mode, "reconnect": reconnect}, timeout=2)
        except Exception as e:
            print(f"Warning: Failed to reach {endpoint}: {e}")

def add_topic_partitions(new_total=12):
    print(f"[*] Adding partitions (new total: {new_total})...")
    subprocess.run([
        "docker", "compose", "exec", "-t", "kafka",
        "kafka-topics.sh", "--alter", "--topic", "test-topic",
        "--partitions", str(new_total), "--bootstrap-server", "localhost:9092"
    ])

def main():
    print("Starting simulation framework for Class 1 (Rebalance Loop)....")

    subprocess.run(["docker", "compose", "up", "-d", "--build"], check=True)
    print("Waiting 20 seconds for Kafka, Producer, and Consumers to warm up...")
    time.sleep(20)

    phases = []

    cycles = 1
    partition_count = 6
    # duration_per_phase = 10 * 60
    duration_per_phase = 1 * 30  # 10 minutes per phase for real runs, can be reduced for testing

    for cycle in range(cycles):
        print(f"\n--- Starting Cycle {cycle+1}/{cycles} ---")

        print("[*] Entering Healthy State")
        set_consumers_state(fault_mode="healthy")
        start_time = utc_now()
        time.sleep(duration_per_phase)
        phases.append((start_time, utc_now(), "healthy", "none"))

        print("[*] Entering Fault State: Explicit Connections Rebalancing")
        start_time = utc_now()
        for _ in range(duration_per_phase // 20):
            set_consumers_state(reconnect=True, target_consumers=[CONSUMERS[0]])
            time.sleep(20)
        phases.append((start_time, utc_now(), "fault", "explicit_reconnects"))

        print("[*] Returning to Healthy State")
        set_consumers_state(fault_mode="healthy")
        start_time = utc_now()
        time.sleep(duration_per_phase)
        phases.append((start_time, utc_now(), "healthy", "none"))

        print("[*] Entering Fault State: Consumer Timeouts")
        set_consumers_state(fault_mode="consumer_timeout", target_consumers=[CONSUMERS[1]])
        start_time = utc_now()
        time.sleep(duration_per_phase)
        phases.append((start_time, utc_now(), "fault", "consumer_timeouts"))

        print("[*] Returning to Healthy State")
        set_consumers_state(fault_mode="healthy")
        start_time = utc_now()
        time.sleep(duration_per_phase)
        phases.append((start_time, utc_now(), "healthy", "none"))

        print("[*] Entering Fault State: Topic Partition Additions")
        start_time = utc_now()
        for i in range(duration_per_phase // 60):
            partition_count += 1
            add_topic_partitions(new_total=partition_count)
            time.sleep(60)
        phases.append((start_time, utc_now(), "fault", "metadata_changes"))

    print("\nSimulation complete. Shutting down environment...")

    print("Exporting metrics database from the scraper container...")
    try:
        container_id_cmd = subprocess.run(["docker", "compose", "ps", "-q", "scraper"], capture_output=True, text=True)
        container_id = container_id_cmd.stdout.strip()

        subprocess.run(["docker", "cp", f"{container_id}:/app/data/metrics.db", "metrics.db"])
        print("Metrics DB exported successfully")
    except Exception as e:
        print(f"Error copying metrics DB: {e}")

    print("Generating labels.csv mapping scrape_id to fault labels...")
    try:
        conn = sqlite3.connect("metrics.db")
        cur = conn.cursor()
        cur.execute("SELECT id, scraped_at FROM scrape_sessions")
        rows = cur.fetchall()

        with open("labels.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["scrape_id", "label"])
            for row in rows:
                s_id = row[0]
                s_time = row[1]

                # Determine label by checking which phase timeframe the scrape occurred in
                current_label = "unknown"
                for start, end, state, fault_cause in phases:
                    # ISO-8601 strings allow for valid lexicographical time comparisons
                    if start <= s_time <= end:
                        current_label = 1 if fault_cause != "none" else 0
                        break
                if not (current_label == "unknown"):
                    writer.writerow([s_id, current_label])
        print("labels.csv generated successfully.")
    except Exception as e:
        print(f"Error generating labels.csv: {e}")

    subprocess.run(["docker", "compose", "down"])

if __name__ == "__main__":
    main()
