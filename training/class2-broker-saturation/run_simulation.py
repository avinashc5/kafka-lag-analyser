import subprocess
import time
import csv
import requests
import os
import sqlite3
from datetime import datetime, timezone

PRODUCER = "http://localhost:6001/setState"

def utc_now():
    return datetime.now(timezone.utc).isoformat()

def set_producer_state(fault_mode="healthy"):
    try:
        requests.post(PRODUCER, json={"fault_mode": fault_mode}, timeout=2)
    except Exception as e:
        print(f"Warning: Failed to reach {PRODUCER}: {e}")

def set_broker_cpu(cpus="0.0"):
    print(f"[*] Setting broker CPU limit to {cpus} (0.0=unlimited)...")
    try:
        container_id_cmd = subprocess.run(["docker", "compose", "ps", "-q", "kafka"], capture_output=True, text=True)
        container_id = container_id_cmd.stdout.strip()
        if container_id:
            # Docker update limits dynamically at runtime!
            subprocess.run(["docker", "update", f"--cpus={cpus}", container_id])
    except Exception as e:
        print(f"Error setting CPU limits: {e}")

def main():
    print("Starting simulation framework for Class 2 (Broker Saturation)....")

    subprocess.run(["docker", "compose", "up", "-d", "--build"], check=True)
    print("Waiting 30 seconds for Kafka, Producer, and Consumers to warm up...")
    time.sleep(30)

    phases = []

    cycles = 2
    duration_per_phase = 10 * 60 # 10 minutes per phase for real runs, can be reduced for testing

    for cycle in range(cycles):
        print(f"\n--- Starting Cycle {cycle+1}/{cycles} ---")

        print("[*] Entering Healthy State")
        set_producer_state(fault_mode="healthy")
        set_broker_cpu(cpus="0.0")
        start_time = utc_now()
        time.sleep(duration_per_phase)
        phases.append((start_time, utc_now(), "healthy", "none"))

        print("[*] Entering Fault State: CPU Saturation")
        set_broker_cpu(cpus="0.05") # Drop immediately to 5% CPU capacity
        start_time = utc_now()
        time.sleep(duration_per_phase)
        phases.append((start_time, utc_now(), "fault", "cpu_saturation"))

        print("[*] Returning to Healthy State")
        set_broker_cpu(cpus="0.0")
        start_time = utc_now()
        time.sleep(duration_per_phase)
        phases.append((start_time, utc_now(), "healthy", "none"))

        print("[*] Entering Fault State: Disk I/O Saturation")
        set_producer_state(fault_mode="disk_io_saturation") # Command producer to dump 5MB random byte payloads
        start_time = utc_now()
        time.sleep(duration_per_phase)
        phases.append((start_time, utc_now(), "fault", "disk_io_saturation"))

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
