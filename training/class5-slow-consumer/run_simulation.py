import subprocess
import time
import csv
import requests
import sqlite3
from datetime import datetime, timezone

CONSUMERS = [
    "http://localhost:5001/setState",
    "http://localhost:5002/setState"
]

def utc_now():
    return datetime.now(timezone.utc).isoformat()

def set_consumers_state(fault_mode="healthy"):
    for endpoint in CONSUMERS:
        try:
            requests.post(endpoint, json={"fault_mode": fault_mode}, timeout=2)
        except Exception as e:
            print(f"Warning: Failed to reach {endpoint}: {e}")

def main():
    print("Starting simulation framework for Class 5 (Slow Consumer)....")

    subprocess.run(["docker", "compose", "up", "-d", "--build"], check=True)
    print("Waiting 30 seconds for cluster, producer, and consumers to warm up...")
    time.sleep(30)

    phases = []
    cycles = 10
    duration_per_phase = 2 * 60

    for cycle in range(cycles):
        print(f"\n--- Starting Cycle {cycle+1}/{cycles} ---")

        print("[*] Entering Healthy State")
        set_consumers_state(fault_mode="healthy")
        start_time = utc_now()
        time.sleep(duration_per_phase)
        phases.append((start_time, utc_now(), "healthy", "none"))

        print("[*] Entering Fault State: Slow Consumer")
        set_consumers_state(fault_mode="slow_consumer")
        start_time = utc_now()
        time.sleep(duration_per_phase)
        phases.append((start_time, utc_now(), "fault", "slow_consumer"))

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
                        current_label = fault_cause if fault_cause != 1 else 0
                        break
                if not current_label == "unknown":
                    writer.writerow([s_id, current_label])

        print("labels.csv generated successfully.")
    except Exception as e:
        print(f"Error generating labels.csv: {e}")

    subprocess.run(["docker", "compose", "down"])

if __name__ == "__main__":
    main()
