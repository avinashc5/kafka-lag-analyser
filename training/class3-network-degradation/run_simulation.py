import subprocess
import time
import csv
import os
import sqlite3
from datetime import datetime, timezone

def utc_now():
    return datetime.now(timezone.utc).isoformat()

def set_network_delay(container, delay="200ms", enable=True):
    if enable:
        print(f"[*] Adding {delay} delay to {container}")
        # Ignore error if already added
        subprocess.run(["docker", "compose", "exec", "-t", container, "tc", "qdisc", "add", "dev", "eth0", "root", "netem", "delay", delay], capture_output=True)
    else:
        print(f"[*] Removing delay from {container}")
        subprocess.run(["docker", "compose", "exec", "-t", container, "tc", "qdisc", "del", "dev", "eth0", "root", "netem"], capture_output=True)

def main():
    print("Starting simulation framework for Class 3 (Network Degradation)....")

    subprocess.run(["docker", "compose", "up", "-d", "--build"], check=True)
    print("Waiting 30 seconds for cluster, producer, and consumer to warm up...")
    time.sleep(30)

    phases = []
    cycles = 10
    duration_per_phase = 1 * 60

    for cycle in range(cycles):
        print(f"\n--- Starting Cycle {cycle+1}/{cycles} ---")

        print("[*] Entering Healthy State")
        set_network_delay("consumer", enable=False)
        set_network_delay("producer", enable=False)
        start_time = utc_now()
        time.sleep(duration_per_phase)
        phases.append((start_time, utc_now(), "healthy", "none"))

        print("[*] Entering Fault State: Network Degradation")
        # Spike client->broker latency (affects fetch and produce total time)
        set_network_delay("consumer", delay="400ms", enable=True)
        set_network_delay("producer", delay="400ms", enable=True)
        start_time = utc_now()
        time.sleep(duration_per_phase)
        phases.append((start_time, utc_now(), "fault", "network_degradation"))

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
                if not (current_label == "unknown"):
                    writer.writerow([s_id, current_label])
        print("labels.csv generated successfully.")
    except Exception as e:
        print(f"Error generating labels.csv: {e}")

    subprocess.run(["docker", "compose", "down"])

if __name__ == "__main__":
    main()
