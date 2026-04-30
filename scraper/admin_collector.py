from dataclasses import dataclass

import time
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError


@dataclass
class GroupLagSample:
    group_id: str
    topic: str
    partition: int
    committed_offset: int
    log_end_offset: int
    lag: int
    group_state: str   # Stable | PreparingRebalance | CompletingRebalance | Dead | Empty


class AdminCollector:
    """
    Queries the Kafka broker (via Admin API) for per-partition consumer lag and
    group state.  No connection is made to producers or consumers directly;
    everything is fetched from the broker's Group Coordinator.
    """

    def __init__(self, bootstrap_servers: list):
        self._bootstrap = bootstrap_servers
        self._admin: KafkaAdminClient = None
        self._consumer: KafkaConsumer = None

    def connect(self):
        while True:
            try:
                self._admin = KafkaAdminClient(bootstrap_servers=self._bootstrap)
                break
            except Exception as e:
                print(f"[Scraper] Waiting for Kafka... {e}")
                time.sleep(3)
        # A no-group consumer used solely for end_offsets() queries.
        self._consumer = KafkaConsumer(
            bootstrap_servers=self._bootstrap,
            enable_auto_commit=False,
        )

    def close(self):
        if self._consumer:
            self._consumer.close()
        if self._admin:
            self._admin.close()

    def collect(self) -> list:
        """
        Returns a list of GroupLagSample, one per (group, topic, partition).
        Groups that fail individually are skipped rather than aborting the whole call.
        """
        results = []

        try:
            groups = self._admin.list_consumer_groups()
        except KafkaError as exc:
            raise RuntimeError(f"Cannot list consumer groups: {exc}") from exc

        for group_id, _ in groups:
            try:
                results.extend(self._collect_group(group_id))
            except KafkaError:
                pass  # one bad group should not stop the rest

        return results

    # ── internal ──────────────────────────────────────────────────────────────

    def _collect_group(self, group_id: str) -> list:
        committed = self._admin.list_consumer_group_offsets(group_id)
        if not committed:
            return []

        topic_partitions = list(committed.keys())
        end_offsets = self._consumer.end_offsets(topic_partitions)

        descriptions = self._admin.describe_consumer_groups([group_id])
        state = descriptions[0].state if descriptions else "Unknown"

        samples = []
        for tp, offset_meta in committed.items():
            committed_offset = offset_meta.offset if offset_meta else 0
            log_end_offset   = end_offsets.get(tp, committed_offset)
            samples.append(GroupLagSample(
                group_id=group_id,
                topic=tp.topic,
                partition=tp.partition,
                committed_offset=committed_offset,
                log_end_offset=log_end_offset,
                lag=max(0, log_end_offset - committed_offset),
                group_state=state,
            ))
        return samples
