"""
Kafka Consumer Lag Root-Cause Analyser
Collects metrics, builds labeled dataset, trains XGBoost classifier,
and runs live inference — all in one pipeline.
"""

import time
import os
import json
import csv
import numpy as np
import pandas as pd
from kafka import KafkaConsumer, KafkaAdminClient, TopicPartition
from kafka.admin import KafkaAdminClient
from collections import defaultdict, deque
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report
import warnings

warnings.filterwarnings("ignore")

BOOTSTRAP = os.environ.get("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "test-topic"
GROUP_ID = "group1"
FAULT_MODE = os.environ.get("FAULT_MODE", "healthy")  # label for training
MODE = os.environ.get("ANALYZER_MODE", "collect")  # 'collect' or 'infer'
COLLECT_SECONDS = int(os.environ.get("COLLECT_SECONDS", "60"))
POLL_INTERVAL = 2  # seconds between metric snapshots
DATASET_FILE = "/data/kafka_metrics.csv"
MODEL_FILE = "/data/model.json"

# ─────────────────────────────────────────────
# METRIC COLLECTION
# ─────────────────────────────────────────────


def wait_for_kafka():
    while True:
        try:
            admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
            print("[Analyser] Connected to Kafka")
            return admin
        except Exception as e:
            print(f"[Analyser] Waiting for Kafka... {e}")
            time.sleep(3)


def get_partition_offsets(admin, consumer_tmp):
    """
    Returns per-partition lag, end offsets, and committed offsets.
    consumer_tmp is a throwaway KafkaConsumer for offset queries.
    """
    # Get all partitions for topic
    partitions = consumer_tmp.partitions_for_topic(TOPIC)
    if not partitions:
        return {}

    tps = [TopicPartition(TOPIC, p) for p in partitions]

    # End offsets (log end offset = latest message position)
    end_offsets = consumer_tmp.end_offsets(tps)

    # Committed offsets for our consumer group
    committed = {}
    for tp in tps:
        c = consumer_tmp.committed(tp)
        committed[tp] = c if c is not None else 0

    result = {}
    for tp in tps:
        end = end_offsets[tp]
        comm = committed[tp]
        result[tp.partition] = {
            "end_offset": end,
            "committed_offset": comm,
            "lag": max(0, end - comm),
        }

    return result


def collect_snapshot(admin, consumer_tmp, prev_end_offsets, prev_time):
    """
    Collect one snapshot of features. Returns a feature dict.
    """
    now = time.time()
    elapsed = now - prev_time if prev_time else POLL_INTERVAL

    partition_data = get_partition_offsets(admin, consumer_tmp)
    if not partition_data:
        return None, prev_end_offsets, now

    lags = [p["lag"] for p in partition_data.values()]
    end_offsets = {pid: p["end_offset"] for pid, p in partition_data.items()}
    committed_offsets = {
        pid: p["committed_offset"] for pid, p in partition_data.items()
    }

    # ── Lag features ──
    total_lag = sum(lags)
    max_lag = max(lags)
    min_lag = min(lags)
    mean_lag = np.mean(lags)
    std_lag = np.std(lags)
    # Coefficient of variation → high = imbalance
    lag_cv = std_lag / (mean_lag + 1e-9)

    # ── Throughput features ──
    # How many new messages arrived since last snapshot
    throughput_per_partition = {}
    if prev_end_offsets:
        for pid, eo in end_offsets.items():
            prev_eo = prev_end_offsets.get(pid, eo)
            throughput_per_partition[pid] = max(0, eo - prev_eo) / elapsed
    else:
        throughput_per_partition = {pid: 0 for pid in end_offsets}

    total_throughput = sum(throughput_per_partition.values())
    throughput_std = np.std(list(throughput_per_partition.values()))
    # High throughput_std → some partitions getting more data (imbalance)
    throughput_cv = throughput_std / (
        total_throughput / len(throughput_per_partition) + 1e-9
    )

    # ── Commit features ──
    # Compare committed offsets to end offsets per partition
    # If commit is far behind end offset and not advancing → commit failure
    commit_gaps = [
        max(0, end_offsets[pid] - committed_offsets.get(pid, 0)) for pid in end_offsets
    ]
    mean_commit_gap = np.mean(commit_gaps)
    max_commit_gap = max(commit_gaps)

    # ── Partition imbalance features ──
    # How concentrated is the data? Gini-like measure
    tp_values = list(throughput_per_partition.values())
    tp_total = sum(tp_values) + 1e-9
    tp_fractions = sorted([v / tp_total for v in tp_values])
    # Perfect balance = all fractions equal. Gini coefficient:
    n = len(tp_fractions)
    gini = (
        (
            2
            * sum((i + 1) * v for i, v in enumerate(tp_fractions))
            / (n * tp_total * (tp_total))
        )
        if tp_total > 1e-8
        else 0
    )
    # Simpler: max partition share
    max_partition_share = max(tp_fractions)

    # Active partitions (those receiving > 0 messages)
    active_partitions = sum(1 for v in tp_values if v > 0)
    total_partitions = len(tp_values)
    active_ratio = active_partitions / total_partitions

    features = {
        # Lag
        "total_lag": total_lag,
        "max_lag": max_lag,
        "min_lag": min_lag,
        "mean_lag": mean_lag,
        "std_lag": std_lag,
        "lag_cv": lag_cv,  # High → imbalance
        # Throughput
        "total_throughput": total_throughput,
        "throughput_cv": throughput_cv,  # High → imbalance
        "throughput_std": throughput_std,
        # Commit
        "mean_commit_gap": mean_commit_gap,  # High → commit failure
        "max_commit_gap": max_commit_gap,
        # Partition distribution
        "max_partition_share": max_partition_share,  # Near 1.0 → all traffic on one partition
        "active_ratio": active_ratio,  # Low → many idle partitions
        "num_partitions": total_partitions,
    }

    return features, end_offsets, now


# ─────────────────────────────────────────────
# DATASET BUILDING
# ─────────────────────────────────────────────


def collect_mode(admin, consumer_tmp):
    """
    Run for COLLECT_SECONDS, writing labeled snapshots to CSV.
    FAULT_MODE env var is the label.
    """
    print(f"[Analyser] COLLECT MODE | label={FAULT_MODE} | duration={COLLECT_SECONDS}s")
    os.makedirs("/data", exist_ok=True)

    file_exists = os.path.isfile(DATASET_FILE)
    prev_end_offsets = {}
    prev_time = None
    snapshots = []

    deadline = time.time() + COLLECT_SECONDS
    while time.time() < deadline:
        features, prev_end_offsets, prev_time = collect_snapshot(
            admin, consumer_tmp, prev_end_offsets, prev_time
        )
        if features:
            features["label"] = FAULT_MODE
            snapshots.append(features)
            print(
                f"[Analyser] Snapshot: lag={features['total_lag']:.0f} "
                f"throughput={features['total_throughput']:.1f} "
                f"lag_cv={features['lag_cv']:.2f} "
                f"max_share={features['max_partition_share']:.2f}"
            )
        time.sleep(POLL_INTERVAL)

    # Write to CSV
    if snapshots:
        df = pd.DataFrame(snapshots)
        df.to_csv(DATASET_FILE, mode="a", header=not file_exists, index=False)
        print(f"[Analyser] Wrote {len(snapshots)} rows to {DATASET_FILE}")


# ─────────────────────────────────────────────
# TRAINING
# ─────────────────────────────────────────────


def train_mode():
    """
    Load CSV, train XGBoost, print evaluation, save model.
    """
    print("[Analyser] TRAIN MODE")
    df = pd.read_csv(DATASET_FILE)
    print(f"[Analyser] Dataset: {len(df)} rows, label distribution:")
    print(df["label"].value_counts())

    feature_cols = [c for c in df.columns if c != "label"]
    X = df[feature_cols].values
    le = LabelEncoder()
    y = le.fit_transform(df["label"].values)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = xgb.XGBClassifier(
        n_estimators=200,
        max_depth=5,
        learning_rate=0.1,
        use_label_encoder=False,
        eval_metric="mlogloss",
        random_state=42,
    )
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    y_pred = model.predict(X_test)
    print("\n[Analyser] Classification Report:")
    print(classification_report(y_test, y_pred, target_names=le.classes_))

    # Save model + label mapping
    model.save_model(MODEL_FILE)
    label_map = {int(i): cls for i, cls in enumerate(le.classes_)}
    with open("/data/label_map.json", "w") as f:
        json.dump(label_map, f)

    print(f"[Analyser] Model saved to {MODEL_FILE}")

    # Feature importance
    importance = model.feature_importances_
    feat_importance = sorted(zip(feature_cols, importance), key=lambda x: -x[1])
    print("\n[Analyser] Feature Importances:")
    for feat, imp in feat_importance:
        bar = "█" * int(imp * 40)
        print(f"  {feat:<25} {bar} {imp:.4f}")


# ─────────────────────────────────────────────
# LIVE INFERENCE
# ─────────────────────────────────────────────


def infer_mode(admin, consumer_tmp):
    """
    Load saved model, continuously collect metrics, classify in real-time.
    """
    print("[Analyser] INFER MODE — loading model...")
    model = xgb.XGBClassifier()
    model.load_model(MODEL_FILE)
    with open("/data/label_map.json") as f:
        label_map = json.load(f)
    # XGBoost returns int keys
    label_map = {int(k): v for k, v in label_map.items()}

    feature_cols = [
        "total_lag",
        "max_lag",
        "min_lag",
        "mean_lag",
        "std_lag",
        "lag_cv",
        "total_throughput",
        "throughput_cv",
        "throughput_std",
        "mean_commit_gap",
        "max_commit_gap",
        "max_partition_share",
        "active_ratio",
        "num_partitions",
    ]

    # Rolling window for smoothing predictions
    recent_preds = deque(maxlen=5)
    prev_end_offsets = {}
    prev_time = None

    print("[Analyser] Starting live inference loop...\n")
    while True:
        features, prev_end_offsets, prev_time = collect_snapshot(
            admin, consumer_tmp, prev_end_offsets, prev_time
        )
        if not features:
            time.sleep(POLL_INTERVAL)
            continue

        x = np.array([[features[c] for c in feature_cols]])
        pred_class = int(model.predict(x)[0])
        pred_proba = model.predict_proba(x)[0]
        confidence = float(np.max(pred_proba))
        diagnosis = label_map[pred_class]

        recent_preds.append(pred_class)
        # Majority vote over rolling window
        from collections import Counter

        stable_pred = Counter(recent_preds).most_common(1)[0][0]
        stable_diagnosis = label_map[stable_pred]

        print("─" * 55)
        print(f"  Diagnosis (raw)    : {diagnosis} ({confidence:.1%} confidence)")
        print(
            f"  Diagnosis (stable) : {stable_diagnosis}  [window={len(recent_preds)}]"
        )
        print(f"  Total Lag          : {features['total_lag']:.0f}")
        print(f"  Throughput         : {features['total_throughput']:.1f} msg/s")
        print(f"  Lag CV             : {features['lag_cv']:.3f}")
        print(f"  Max Partition Share: {features['max_partition_share']:.3f}")
        print(f"  Commit Gap (mean)  : {features['mean_commit_gap']:.1f}")
        print("─" * 55)
        time.sleep(POLL_INTERVAL)


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    admin = wait_for_kafka()

    # Throwaway consumer just for offset queries (not consuming messages)
    consumer_tmp = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
    )

    if MODE == "collect":
        collect_mode(admin, consumer_tmp)
    elif MODE == "train":
        train_mode()
    elif MODE == "infer":
        infer_mode(admin, consumer_tmp)
    else:
        print(f"[Analyser] Unknown MODE={MODE}. Use: collect | train | infer")
