import subprocess
import time
import csv
import requests
import os
import sqlite3
from datetime import datetime, timezone

PRODUCER = "http://localhost:5001/setState"

def utc_now():
    return datetime.now(timezone.utc).isoformat()

def set_producer_state(fault_mode="healthy"):
    try:
        requests.post(PRODUCER, json={"fault_mode": fault_mode}, timeout=2)
    except Exception as e:
        print(f"Warning: Failed to reach {PRODUCER}: {e}")

def main():
    print("Starting simulation framework for Class 4 (Partition Skew)....")

    subprocess.run(["docker", "compose", "up", "-d", "--build"], check=True)
    print("Waiting 30 seconds for Kafka, Producer, and Consumers to warm up...")
    time.sleep(30)

    phases = []
    duration_per_phase = 10 * 60

    # Use just ONE cycle because size_cv (accumulated size skew) does not naturally
    # revert cleanly to a healthy threshold after a heavy skew phase.
    print(f"\n--- Starting 1-Cycle Run ---")

    print("[*] Entering Healthy State")
    set_producer_state(fault_mode="healthy")
    start_time = utc_now()
    time.sleep(duration_per_phase)
    phases.append((start_time, utc_now(), "healthy", "none"))

    print("[*] Entering Fault State: Partition Skew")
    set_producer_state(fault_mode="partition_skew")
    start_time = utc_now()
    time.sleep(duration_per_phase)
    phases.append((start_time, utc_now(), "fault", "partition_skew"))

    print("\nSimulation complete. Shutting down environment...")

    print("Exporting metrics database from the scraper container...")
    try:
        container_id_cmd = subprocess.run(["docker", "compose", "ps", "-q", "scraper"], capture_output=True, text=True)
        container_id = container_id_cmd.stdout.strip()
        subprocess.run(["docker", "cp", f"{container_id}:/app/data/metrics.db", "metrics.db"])
        print(f"Metrics DB exported successfully")
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

                current_label = "unknown"
                for start, end, state, fault_cause in phases:
                    if start <= s_time <= end:
                        current_label = fault_cause if fault_cause != "none" else "healthy"
                        break
                if not current_label == "unknown":
                    writer.writerow([s_id, current_label])
        print("labels.csv generated successfully.")
    except Exception as e:
        print(f"Error generating labels.csv: {e}")

    subprocess.run(["docker", "compose", "down"])

if __name__ == "__main__":
    main()
