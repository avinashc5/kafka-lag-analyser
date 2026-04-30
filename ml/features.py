"""
Feature extractors for each fault model.

Each extractor returns a DataFrame whose first columns identify the entity
(scrape_id + optional group_id / topic) and whose remaining columns are the
features.  All numeric columns are filled with 0 where data is absent.

Entity granularities
--------------------
slow_consumer     → (scrape_id, group_id)
rebalance_loops   → (scrape_id, group_id)
partition_skew    → (scrape_id, topic)
broker_saturation → (scrape_id,)
network_delay     → (scrape_id,)
"""

import numpy as np
import pandas as pd

from .data import WINDOW, build_broker_frame

# ── Rolling helpers ────────────────────────────────────────────────────────────


def _rolling_slope(s: pd.Series, w: int) -> pd.Series:
    """Linear regression slope of s over the last w observations."""
    return s.rolling(w, min_periods=2).apply(
        lambda y: np.polyfit(np.arange(len(y)), y, 1)[0],
        raw=True,
    )


def _safe_cv(arr: np.ndarray) -> float:
    """Coefficient of Variation, zero-safe."""
    m = arr.mean()
    return arr.std() / m if m > 0 else 0.0


# ── 1. slow_consumer ──────────────────────────────────────────────────────────


def extract_slow_consumer(jmx: pd.DataFrame, lag: pd.DataFrame) -> pd.DataFrame:
    """
    Per (scrape_id, group_id, partition).

    Features
    --------
    total_lag              : absolute consumer group lag
    lag_growth_slope       : rolling linear slope of lag (positive = falling behind)
    lag_delta_std          : rolling std of lag changes (erratic vs sustained growth)
    lag_normalized         : total_lag / broker-wide msgs_in_per_sec
    committed_delta_rate   : rolling mean of Δcommitted_offset (processing throughput proxy)
    # stable_frac            : fraction of partitions in Stable state
    lag_max_jump           : largest single-step lag increase in window
    """
    W = WINDOW

    grp = (
        lag.groupby(["scrape_id", "group_id"])
        .agg(
            total_lag=("lag", "sum"),
            total_committed=("committed_offset", "sum"),
            stable_frac=("group_state", lambda s: (s == "Stable").mean()),
        )
        .reset_index()
        .sort_values(["group_id", "scrape_id"])
    )

    def group_roll(df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values("scrape_id").copy()
        df["lag_delta"] = df["total_lag"].diff()
        df["committed_delta"] = df["total_committed"].diff()
        df["lag_growth_slope"] = _rolling_slope(df["total_lag"], W)
        df["lag_delta_std"] = df["lag_delta"].rolling(W, min_periods=2).std()
        df["committed_delta_rate"] = df["committed_delta"].rolling(W, min_periods=1).mean()
        df["lag_max_jump"] = df["lag_delta"].rolling(W, min_periods=1).max()
        return df

    grp = grp.groupby(["group_id"], group_keys=False)[grp.columns].apply(group_roll)

    # Broker-wide msgs_in for normalisation
    msgs_in = (
        jmx[
            (jmx["metric_name"] == "kafka_server_brokertopicmetrics_messagesinpersec_oneminuterate")
            & jmx["topic"].isna()
        ]
        .groupby("scrape_id")["value"]
        .sum()
        .rename("msgs_in_total")
    )
    grp = grp.merge(msgs_in.reset_index(), on="scrape_id", how="left")
    grp["lag_normalized"] = grp["total_lag"] / grp["msgs_in_total"].replace(0, np.nan).fillna(0)

    cols = [
        "scrape_id", "group_id",
        "total_lag", "lag_growth_slope", "lag_delta_std",
        "lag_normalized", "committed_delta_rate", "stable_frac", "lag_max_jump",
    ]
    grp = grp.dropna(subset=["lag_growth_slope"])
    return grp[cols]

# ── 3. rebalance_loops ───────────────────────────────────────────────────────


def extract_rebalance_loops(jmx: pd.DataFrame, lag: pd.DataFrame) -> pd.DataFrame:
    """
    Per (scrape_id, group_id).

    Broker-level rebalance signals are identical for every group at a given
    scrape; per-group columns capture state instability and lag oscillation.

    Features
    --------
    joingroup_rate        : JoinGroup requests/sec (direct rebalance trigger rate)
    syncgroup_rate        : SyncGroup requests/sec
    joingroup_latency_p99 : TotalTimeMs P99 for JoinGroup (expensive rebalances compound loops)
    preparing_ratio       : NumGroupsPreparingRebalance / NumGroups
    state_instability     : rolling mean of non-Stable fraction for this group
    lag_oscillation       : rolling std of Δlag for this group (spike/recover pattern)
    lag_mean              : rolling mean of lag (context: is the group actually behind?)
    """
    W = WINDOW

    # ── Per-group lag features ──
    grp = (
        lag.groupby(["scrape_id", "group_id"])
        .agg(
            total_lag=("lag", "sum"),
            non_stable_frac=("group_state", lambda s: (s != "Stable").mean()),
        )
        .reset_index()
        .sort_values(["group_id", "scrape_id"])
    )

    def group_roll(df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values("scrape_id").copy()
        df["lag_delta"] = df["total_lag"].diff()
        df["state_instability"] = df["non_stable_frac"].rolling(W, min_periods=1).mean()
        df["lag_oscillation"] = df["lag_delta"].rolling(W, min_periods=2).std()
        df["lag_mean"] = df["total_lag"].rolling(W, min_periods=1).mean()
        return df

    grp = grp.groupby("group_id", group_keys=False)[grp.columns].apply(group_roll)

    # ── Broker-level JMX (same value broadcast to every group at that scrape) ──
    scrape_ids = grp["scrape_id"].unique()

    joingroup_rate = (
        jmx[
            (jmx["metric_name"] == "kafka_network_requestmetrics_requestspersec_oneminuterate")
            & (jmx["request"] == "JoinGroup")
        ]
        .groupby("scrape_id")["value"].first().rename("joingroup_rate")
    )
    syncgroup_rate = (
        jmx[
            (jmx["metric_name"] == "kafka_network_requestmetrics_requestspersec_oneminuterate")
            & (jmx["request"] == "SyncGroup")
        ]
        .groupby("scrape_id")["value"].first().rename("syncgroup_rate")
    )
    joingroup_latency = (
        jmx[
            (jmx["metric_name"] == "kafka_network_requestmetrics_totaltimems_99thpercentile")
            & (jmx["request"] == "JoinGroup")
        ]
        .groupby("scrape_id")["value"].first().rename("joingroup_latency_p99")
    )
    num_preparing = (
        jmx[jmx["metric_name"] == "kafka_coordinator_group_groupmetadatamanager_numgroupspreparingrebalance"]
        .groupby("scrape_id")["value"].first().rename("num_preparing")
    )
    num_groups = (
        jmx[jmx["metric_name"] == "kafka_coordinator_group_groupmetadatamanager_numgroups"]
        .groupby("scrape_id")["value"].first().rename("num_groups")
    )

    broker = (
        pd.concat([joingroup_rate, syncgroup_rate, joingroup_latency, num_preparing, num_groups], axis=1)
        .reindex(scrape_ids)
        .fillna(0)
        .reset_index()
        .rename(columns={"index": "scrape_id"})
    )
    broker["preparing_ratio"] = broker["num_preparing"] / broker["num_groups"].replace(0, np.nan)

    grp = grp.merge(broker, on="scrape_id", how="left")

    cols = [
        "scrape_id", "group_id",
        "joingroup_rate", "syncgroup_rate", "joingroup_latency_p99",
        "preparing_ratio", "state_instability", "lag_oscillation", "lag_mean",
    ]
    return grp[cols].fillna(0)


# ── 4. partition_skew ────────────────────────────────────────────────────────

def extract_partition_skew(jmx: pd.DataFrame, lag: pd.DataFrame) -> pd.DataFrame:
    """
    Per (scrape_id, topic).

    Features
    --------
    delta_cv             : CV of ΔLogEndOffset across partitions (write-rate skew)
    delta_max_min_ratio  : max / min of ΔLogEndOffset (hottest vs coldest partition)
    size_cv              : CV of partition sizes (accumulated data skew)
    lag_var              : variance of consumer lag across partitions of this topic
    lag_max_ratio        : max_partition_lag / mean_partition_lag
    bytes_in             : topic-level BytesInPerSec (throughput context)
    skew_trend           : rolling slope of delta_cv (is skew worsening?)
    """
    W = WINDOW

    # ── LogEndOffset delta per (topic, partition) ──
    leo = (
        jmx[
            (jmx["metric_name"] == "kafka_log_log_logendoffset")
            & jmx["partition"].notna()
            & jmx["topic"].notna()
        ][["scrape_id", "topic", "partition", "value"]]
        .rename(columns={"value": "leo"})
        .sort_values(["topic", "partition", "scrape_id"])
    )
    leo["leo_delta"] = leo.groupby(["topic", "partition"])["leo"].diff().fillna(0).clip(lower=0)

    # Per (scrape_id, topic): write-rate skew
    def _skew_stats(arr: np.ndarray) -> tuple:
        if len(arr) < 2:
            return 0.0, 1.0
        cv = _safe_cv(arr)
        mn = arr.min()
        ratio = arr.max() / mn if mn > 0 else (arr.max() if arr.max() > 0 else 1.0)
        return cv, ratio

    skew = (
        leo.groupby(["scrape_id", "topic"])["leo_delta"]
        .agg(["std", "mean", "max", "min"])
        .reset_index()
    )
    skew["delta_cv"] = np.where(skew["mean"] > 0, skew["std"] / skew["mean"], 0.0)
    skew["delta_max_min_ratio"] = np.where(
        skew["min"] > 0, skew["max"] / skew["min"],
        np.where(skew["max"] > 0, skew["max"], 1.0),
    )
    skew = skew[["scrape_id", "topic", "delta_cv", "delta_max_min_ratio"]]

    # ── Partition size CV ──
    sizes = (
        jmx[
            (jmx["metric_name"] == "kafka_log_log_size")
            & jmx["partition"].notna()
            & jmx["topic"].notna()
        ][["scrape_id", "topic", "partition", "value"]]
        .rename(columns={"value": "size"})
    )
    size_cv = (
        sizes.groupby(["scrape_id", "topic"])["size"]
        .agg(["std", "mean"])
        .reset_index()
    )
    size_cv["size_cv"] = np.where(size_cv["mean"] > 0, size_cv["std"] / size_cv["mean"], 0.0)
    size_cv = size_cv[["scrape_id", "topic", "size_cv"]]

    # ── Lag stats per (scrape_id, topic) ──
    lag_topic = (
        lag.groupby(["scrape_id", "topic"])["lag"]
        .agg(["var", "max", "mean"])
        .reset_index()
        .rename(columns={"var": "lag_var", "max": "lag_max", "mean": "lag_mean"})
    )
    lag_topic["lag_max_ratio"] = lag_topic["lag_max"] / lag_topic["lag_mean"].replace(0, np.nan)
    lag_topic = lag_topic[["scrape_id", "topic", "lag_var", "lag_max_ratio"]]

    # ── Topic-level BytesIn ──
    bytes_in = (
        jmx[
            (jmx["metric_name"] == "kafka_server_brokertopicmetrics_bytesinpersec_oneminuterate")
            & jmx["topic"].notna()
        ][["scrape_id", "topic", "value"]]
        .rename(columns={"value": "bytes_in"})
    )

    # ── Merge ──
    result = (
        skew
        .merge(size_cv, on=["scrape_id", "topic"], how="left")
        .merge(lag_topic, on=["scrape_id", "topic"], how="left")
        .merge(bytes_in, on=["scrape_id", "topic"], how="left")
        .sort_values(["topic", "scrape_id"])
    )

    # Rolling trend: is write-skew worsening?
    result["skew_trend"] = (
        result.groupby("topic")["delta_cv"]
        .transform(lambda s: _rolling_slope(s.sort_index(), W))
    )

    cols = [
        "scrape_id", "topic",
        "delta_cv", "delta_max_min_ratio", "size_cv",
        "lag_var", "lag_max_ratio", "bytes_in", "skew_trend",
    ]
    return result[cols].fillna(0)


# ── 5. broker_saturation ─────────────────────────────────────────────────────


def extract_broker_saturation(jmx: pd.DataFrame, sessions: pd.DataFrame) -> pd.DataFrame:
    """
    Per scrape_id (one broker = global).

    Features
    --------
    throttle_time_produce_p99  : ThrottleTimeMs P99 for Produce (primary throttle signal)
    throttle_time_fetch_p99    : ThrottleTimeMs P99 for FetchConsumer
    request_handler_idle       : RequestHandlerAvgIdlePercent (low = thread exhaustion)
    network_processor_idle     : NetworkProcessorAvgIdlePercent (low = NIO saturation)
    request_queue_time_p99     : RequestQueueTimeMs P99 for FetchConsumer (queue backlog)
    local_time_produce_p99     : LocalTimeMs P99 for Produce (broker compute under load)
    bytes_in_total             : broker-wide BytesInPerSec (ingest load context)
    """
    W = WINDOW
    broker = build_broker_frame(jmx, sessions["id"]).reset_index()

    col_map = {
        "throttle_time_produce_p99":
            "kafka_network_requestmetrics_throttletimems_99thpercentile__produce",
        "throttle_time_fetch_p99":
            "kafka_network_requestmetrics_throttletimems_99thpercentile__fetchconsumer",
        "request_handler_idle":
            "kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent",
        "network_processor_idle":
            "kafka_network_socketserver_networkprocessoravgidlepercent",
        "request_queue_time_p99":
            "kafka_network_requestmetrics_requestqueuetimems_99thpercentile__fetchconsumer",
        "local_time_produce_p99":
            "kafka_network_requestmetrics_localtimems_99thpercentile__produce",
        "bytes_in_total":
            "kafka_server_brokertopicmetrics_bytesinpersec_oneminuterate",
    }

    df = pd.DataFrame({"scrape_id": broker["scrape_id"]})
    for feat, src in col_map.items():
        df[feat] = broker[src].values if src in broker.columns else 0.0

    df = df.sort_values("scrape_id")
    for col in ["throttle_time_produce_p99", "throttle_time_fetch_p99",
                "request_queue_time_p99", "request_handler_idle"]:
        df[f"{col}_trend"] = _rolling_slope(df[col], W)

    return df.fillna(0)


# ── 6. network_delay ─────────────────────────────────────────────────────────


def extract_network_delay(jmx: pd.DataFrame, sessions: pd.DataFrame) -> pd.DataFrame:
    """
    Per scrape_id (global).

    Features
    --------
    remote_time_produce_p99   : RemoteTimeMs P99 for Produce (inter-broker/follower ACK wait)
    response_send_time_p99    : ResponseSendTimeMs P99 for FetchConsumer (broker→consumer link)
    total_time_fetch_p99      : TotalTimeMs P99 for FetchConsumer (end-to-end fetch latency)
    network_component         : total_time_fetch - local_time_produce (estimated non-broker latency)
    under_replicated          : UnderReplicatedPartitions (replicas can't keep up = network slow)
    replica_maxlag            : ReplicaFetcherManager MaxLag (inter-broker lag)
    replication_ratio         : ReplicationBytesOutPerSec / BytesInPerSec (replication health)
    """
    W = WINDOW
    broker = build_broker_frame(jmx, sessions["id"]).reset_index()

    col_map = {
        "remote_time_produce_p99":
            "kafka_network_requestmetrics_remotetimems_99thpercentile__produce",
        "response_send_time_p99":
            "kafka_network_requestmetrics_responsesendtimems_99thpercentile__fetchconsumer",
        "total_time_fetch_p99":
            "kafka_network_requestmetrics_totaltimems_99thpercentile__fetchconsumer",
        "local_time_produce_p99":
            "kafka_network_requestmetrics_localtimems_99thpercentile__produce",
        "under_replicated":
            "kafka_server_replicamanager_underreplicatedpartitions",
        "replica_maxlag":
            "kafka_server_replicafetchermanager_maxlag",
        "bytes_in_total":
            "kafka_server_brokertopicmetrics_bytesinpersec_oneminuterate",
        "replication_bytes_out":
            "kafka_server_brokertopicmetrics_replicationbytesoutpersec_oneminuterate",
    }

    df = pd.DataFrame({"scrape_id": broker["scrape_id"]})
    for feat, src in col_map.items():
        df[feat] = broker[src].values if src in broker.columns else 0.0

    df = df.sort_values("scrape_id")
    df["network_component"] = (df["total_time_fetch_p99"] - df["local_time_produce_p99"]).clip(lower=0)
    df["replication_ratio"] = df["replication_bytes_out"] / df["bytes_in_total"].replace(0, np.nan)

    for col in ["remote_time_produce_p99", "response_send_time_p99",
                "under_replicated", "replica_maxlag"]:
        df[f"{col}_trend"] = _rolling_slope(df[col], W)

    cols = [
        "scrape_id",
        "remote_time_produce_p99", "response_send_time_p99",
        "total_time_fetch_p99", "network_component",
        "under_replicated", "replica_maxlag", "replication_ratio",
    ]
    return df[cols].fillna(0)
