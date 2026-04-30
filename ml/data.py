"""Data loading utilities for fault model training."""

import json
import sqlite3
from pathlib import Path

import pandas as pd

# ── Constants ──────────────────────────────────────────────────────────────────

TRAINING_DIR = Path("training")
MODELS_DIR = Path("models")
WINDOW = 5  # rolling window size in scrapes

FAULT_CLASSES = [
    "slow_consumer",
    "rebalance_loops",
    "partition_skew",
    "broker_saturation",
    "network_degradation",
]

DIR_MAPPING = {
    "slow_consumer": "class5-slow-consumer",
    "rebalance_loops": "class1-rebalance-loop",
    "broker_saturation": "class2-broker-saturation",
    "network_degradation": "class3-network-degradation",
    "partition_skew": "class4-partition-skew"
}

# ── Loaders ────────────────────────────────────────────────────────────────────


def load_fault_data(fault_class: str) -> tuple:
    """
    Load all tables from a fault class's metrics.db and its labels.csv.

    Returns (sessions, jmx, lag, labels) where:
      - sessions: DataFrame[id]
      - jmx:      DataFrame[scrape_id, metric_name, value, topic, partition, request]
                  (labels JSON is expanded into separate columns)
      - lag:      DataFrame[scrape_id, group_id, topic, partition,
                            committed_offset, log_end_offset, lag, group_state]
      - labels:   DataFrame[scrape_id, fault]
    """
    db_path = TRAINING_DIR / DIR_MAPPING[fault_class] / "metrics.db"
    labels_path = TRAINING_DIR / DIR_MAPPING[fault_class] / "labels.csv"

    conn = sqlite3.connect(db_path)
    sessions = pd.read_sql("SELECT id FROM scrape_sessions ORDER BY id", conn)
    jmx_raw = pd.read_sql(
        "SELECT scrape_id, metric_name, value, labels FROM jmx_samples",
        conn,
    )
    lag = pd.read_sql(
        "SELECT scrape_id, group_id, topic, partition, "
        "committed_offset, log_end_offset, lag, group_state "
        "FROM group_lag_samples",
        conn,
    )
    conn.close()

    # Expand the JSON labels field into separate columns
    parsed = jmx_raw["labels"].apply(json.loads)
    jmx = jmx_raw.drop(columns=["labels"]).copy()
    jmx["topic"] = parsed.apply(lambda d: d.get("topic"))
    jmx["partition"] = parsed.apply(lambda d: d.get("partition"))
    jmx["request"] = parsed.apply(lambda d: d.get("request"))

    labels = pd.read_csv(labels_path)
    return sessions, jmx, lag, labels


# ── JMX helpers ────────────────────────────────────────────────────────────────


def get_metric_series(
    jmx: pd.DataFrame,
    metric_name: str,
    request: str | None = None,
    topic: str | None = None,
    name: str | None = None,
) -> pd.Series:
    """
    Filter jmx to a specific metric (and optionally request type or topic),
    returning a Series indexed by scrape_id.  Missing scrape_ids will be NaN.
    """
    mask = jmx["metric_name"] == metric_name
    if request is not None:
        mask &= jmx["request"] == request
    if topic is not None:
        mask &= jmx["topic"] == topic
    s = jmx[mask].groupby("scrape_id")["value"].first()
    return s.rename(name or metric_name)


def build_broker_frame(jmx: pd.DataFrame, scrape_ids: pd.Series) -> pd.DataFrame:
    """
    Build a DataFrame indexed by scrape_id with all scalar (no-topic,
    no-partition) JMX metrics as columns, plus per-request columns named
    <metric>_<request_type>.  Missing values are filled with 0.
    """
    base = pd.DataFrame({"scrape_id": scrape_ids})

    # ── Global scalars ──
    global_metrics = [
        "kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent",
        "kafka_network_socketserver_networkprocessoravgidlepercent",
        "kafka_server_replicamanager_underreplicatedpartitions",
        "kafka_server_replicafetchermanager_maxlag",
        "kafka_controller_kafkacontroller_offlinepartitionscount",
        "kafka_coordinator_group_groupmetadatamanager_numgroups",
        "kafka_coordinator_group_groupmetadatamanager_numgroupspreparingrebalance",
        "kafka_coordinator_group_groupmetadatamanager_numgroupscompletingrebalance",
        "kafka_coordinator_group_groupmetadatamanager_numgroupsstable",
    ]
    # Also grab broker-level (no topic label) throughput
    throughput_metrics = [
        "kafka_server_brokertopicmetrics_bytesinpersec_oneminuterate",
        "kafka_server_brokertopicmetrics_bytesoutpersec_oneminuterate",
        "kafka_server_brokertopicmetrics_messagesinpersec_oneminuterate",
        "kafka_server_brokertopicmetrics_replicationbytesoutpersec_oneminuterate",
    ]
    for m in global_metrics + throughput_metrics:
        mask = jmx["metric_name"] == m
        # For throughput metrics, pick only the broker-level row (no topic label)
        if m in throughput_metrics:
            mask &= jmx["topic"].isna()
        s = jmx[mask].groupby("scrape_id")["value"].first().rename(m)
        base = base.merge(s.reset_index(), on="scrape_id", how="left")

    # ── Per-request metrics ──
    request_metrics = {
        "kafka_network_requestmetrics_throttletimems_99thpercentile": ["Produce", "FetchConsumer"],
        "kafka_network_requestmetrics_totaltimems_99thpercentile": ["JoinGroup", "FetchConsumer"],
        "kafka_network_requestmetrics_localtimems_99thpercentile": ["Produce"],
        "kafka_network_requestmetrics_remotetimems_99thpercentile": ["Produce"],
        "kafka_network_requestmetrics_responsesendtimems_99thpercentile": ["FetchConsumer"],
        "kafka_network_requestmetrics_requestqueuetimems_99thpercentile": ["FetchConsumer"],
        "kafka_network_requestmetrics_requestspersec_oneminuterate": ["JoinGroup", "SyncGroup"],
    }
    for metric, req_types in request_metrics.items():
        for req in req_types:
            col = f"{metric}__{req.lower()}"
            s = get_metric_series(jmx, metric, request=req, name=col)
            base = base.merge(s.reset_index(), on="scrape_id", how="left")

    return base.set_index("scrape_id").fillna(0)
