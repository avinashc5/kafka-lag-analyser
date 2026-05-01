# Members
Avinash Chaudhari - 23b1064 <br>
Tejas Chaudhari - 23b0932 <br>
Hari Shankar Karthik - 23b0907 <br>
Rishi Kalra - 23b1081 <br>


# Kafka Lag Analyser Setup

This project uses **Kafka 4.1.2**.

Kafka is built **outside** the Docker environment and then copied into the Kafka container.

## Build Kafka

Run:

```bash
cd kafka/
./gradlew jar -x test
```

### Why `-x test`?

The `-x test` flag skips running tests after the build.

Tests are skipped here to reduce setup time and memory usage.

### Running Web UI Demo
1. Build and start everything
docker compose up --build -d

2. Wait ~2 minutes for Kafka to start and scraper to collect enough data (analyser needs at least WINDOW=5 scrapes before inference runs)

3. Open the dashboards
   Fault Analyser:  http://localhost:8000
   Fault Injector:  http://localhost:8001
