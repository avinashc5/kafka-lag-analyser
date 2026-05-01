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

### Running Demo

Run it from the project root with the venv active:


python demo.py slow_consumer
python demo.py rebalance_loops
python demo.py partition_skew
python demo.py broker_saturation
python demo.py network_degradation
Optional flags: --scrapes N (default 13, i.e. ~130s of data), --top N (default top-5 features), --keep-alive (don't tear down after).
