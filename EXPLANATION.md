# Kafka Consumer Lag Root-Cause Analyzer

---

## How to Run

All four stages — Kafka setup, data collection, training, and live inference — are automated in a single script.

```bash
chmod +x run_pipeline.sh
./run_pipeline.sh
```

The full run takes roughly **12–15 minutes** (mostly data collection).

### What the script does, stage by stage

| Stage | What happens |
|---|---|
| 1. Build images | Builds the shared base image (installs all pip packages once), then builds producer, consumer, and analyser images on top |
| 2. Collect data | Runs each fault scenario for 90 s; the analyser writes labeled metric snapshots to `./data/kafka_metrics.csv` |
| 3. Train | Loads the CSV, trains XGBoost, prints a classification report and feature importances |
| 4. Live inference | Starts producer + consumer in healthy mode; analyser prints a diagnosis every 2 s |

### Outputs

| File | Contents |
|---|---|
| `./data/kafka_metrics.csv` | Labeled training dataset (all scenarios combined) |
| `./data/model.json` | Trained XGBoost model |
| `./data/label_map.json` | Integer → class name mapping used at inference time |

> **To retrain without re-collecting data:** comment out the `collect_scenario` calls in `run_pipeline.sh` and run again. The existing CSV will be reused.

> **Port conflict?** If Kafka fails to start, check with `lsof -i :9092` and free the port before retrying.

---

## Fault Classes

The model predicts one of six fault classes plus a healthy baseline.

| Class label | What it means | How it is simulated |
|---|---|---|
| `healthy` | Normal operation, no lag | Producer and consumer running at matching rates |
| `slow_consumer` | Consumer processes messages too slowly | 2-second sleep injected per message in the consumer |
| `partition_imbalance` | All traffic lands on one partition, others idle | Producer uses a fixed key so all messages hash to partition 0 |
| `commit_failure` | Offsets not committed; lag grows despite consumption | Consumer runs with auto-commit disabled and never calls `commit()` |
| `rebalance_loop` | Consumer crashes repeatedly, triggering constant rebalancing | Consumer exits after 20 messages; Docker restart policy re-triggers it |
| `high_throughput` | Producer floods faster than consumer can drain | Producer sends with no sleep; consumer runs at normal speed |

---

## Features Used by the Model

All features are derived from the Kafka Consumer API and `AdminClient` — no JMX or external exporters required. A snapshot is collected every 2 seconds.

| Feature | What it measures | Key signal for |
|---|---|---|
| `total_lag` | Sum of lag across all partitions | `slow_consumer`, `high_throughput` |
| `max_lag` | Highest lag on any single partition | `partition_imbalance` |
| `min_lag` | Lowest lag on any single partition | `partition_imbalance` |
| `mean_lag` | Average lag per partition | general severity |
| `std_lag` | Standard deviation of per-partition lag | `partition_imbalance` |
| `lag_cv` | Coefficient of variation of lag (std / mean); high = uneven distribution | `partition_imbalance` |
| `total_throughput` | New messages arriving per second across all partitions | `high_throughput`, `healthy` |
| `throughput_cv` | Coefficient of variation of per-partition throughput | `partition_imbalance` |
| `throughput_std` | Standard deviation of per-partition throughput | `partition_imbalance` |
| `mean_commit_gap` | Average distance between log-end offset and committed offset | `commit_failure` |
| `max_commit_gap` | Worst-case commit gap on any single partition | `commit_failure` |
| `max_partition_share` | Fraction of total throughput going to the busiest partition (near 1.0 = one partition dominates) | `partition_imbalance` |
| `active_ratio` | Proportion of partitions currently receiving messages | `partition_imbalance` |
| `num_partitions` | Total partition count for the topic | baseline context |

---

## Model

### Algorithm

XGBoost gradient-boosted decision tree classifier (`XGBClassifier`). Chosen because it handles tabular, non-normalised features well, trains fast on small datasets, and produces reliable feature importances that help validate the model is learning the right signals.

### Training configuration

- 200 estimators, max depth 5, learning rate 0.1
- 80/20 train/test split, stratified by class
- Multi-class log-loss as the eval metric
- Label encoding maps class strings to integers; the mapping is saved alongside the model so inference is deterministic

### Inference

In live mode the analyser prints two predictions per snapshot:

- **Raw prediction** — single-snapshot classification with confidence percentage
- **Stable prediction** — majority vote over the last 5 snapshots, smoothing out transient noise at class boundaries

### Sample output

```
───────────────────────────────────────────────────────
  Diagnosis (raw)    : partition_imbalance (91.3% confidence)
  Diagnosis (stable) : partition_imbalance  [window=5]
  Total Lag          : 48200
  Throughput         : 9.8 msg/s
  Lag CV             : 2.341
  Max Partition Share: 0.971
  Commit Gap (mean)  : 8034.2
───────────────────────────────────────────────────────
```
