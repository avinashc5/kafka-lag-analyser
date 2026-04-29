import json
import sqlite3
from pathlib import Path

# ── Schema ─────────────────────────────────────────────────────────────────────
#
# scrape_sessions  – one row per poll tick, timestamps every entry.
#
# jmx_samples      – EAV table: one row per (scrape, metric_name).
#                    labels are stored as a JSON object so any set of Prometheus
#                    label key/value pairs can be kept without schema changes.
#
# group_lag_samples – structured table: one row per (scrape, group, partition).
#                    Kept separate from jmx_samples because the lag data is
#                    structured and will be pivoted directly into ML features.
#
# ──────────────────────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS scrape_sessions (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    scraped_at TEXT    NOT NULL          -- ISO-8601 UTC timestamp
);

CREATE TABLE IF NOT EXISTS jmx_samples (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    scrape_id   INTEGER NOT NULL REFERENCES scrape_sessions(id),
    metric_name TEXT    NOT NULL,
    value       REAL    NOT NULL,
    labels      TEXT    NOT NULL DEFAULT '{}'  -- JSON object
);

CREATE TABLE IF NOT EXISTS group_lag_samples (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    scrape_id        INTEGER NOT NULL REFERENCES scrape_sessions(id),
    group_id         TEXT    NOT NULL,
    topic            TEXT    NOT NULL,
    partition        INTEGER NOT NULL,
    committed_offset INTEGER NOT NULL,
    log_end_offset   INTEGER NOT NULL,
    lag              INTEGER NOT NULL,
    group_state      TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jmx_scrape    ON jmx_samples(scrape_id);
CREATE INDEX IF NOT EXISTS idx_jmx_name      ON jmx_samples(metric_name);
CREATE INDEX IF NOT EXISTS idx_grp_scrape    ON group_lag_samples(scrape_id);
CREATE INDEX IF NOT EXISTS idx_grp_group     ON group_lag_samples(group_id);
CREATE INDEX IF NOT EXISTS idx_session_time  ON scrape_sessions(scraped_at);
"""


class Database:
    def __init__(self, path: str):
        self._path = path
        self._conn: sqlite3.Connection = None

    def connect(self):
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self._path)
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self):
        if self._conn:
            self._conn.close()

    def write_batch(self, batch: list):
        """
        Persist a batch of scrape entries atomically.

        Each entry is a dict:
          {
            'scraped_at': str,               # ISO-8601 timestamp
            'jmx':        list[JMXSample],
            'admin':      list[GroupLagSample],
          }
        """
        for entry in batch:
            cur = self._conn.execute(
                "INSERT INTO scrape_sessions (scraped_at) VALUES (?)",
                (entry["scraped_at"],),
            )
            scrape_id = cur.lastrowid

            if entry["jmx"]:
                self._conn.executemany(
                    "INSERT INTO jmx_samples "
                    "(scrape_id, metric_name, value, labels) VALUES (?, ?, ?, ?)",
                    [
                        (scrape_id, s.metric_name, s.value, json.dumps(s.labels))
                        for s in entry["jmx"]
                    ],
                )

            if entry["admin"]:
                self._conn.executemany(
                    "INSERT INTO group_lag_samples "
                    "(scrape_id, group_id, topic, partition, "
                    " committed_offset, log_end_offset, lag, group_state) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (
                            scrape_id,
                            s.group_id,
                            s.topic,
                            s.partition,
                            s.committed_offset,
                            s.log_end_offset,
                            s.lag,
                            s.group_state,
                        )
                        for s in entry["admin"]
                    ],
                )

        self._conn.commit()
