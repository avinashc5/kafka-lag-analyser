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