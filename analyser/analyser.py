"""
Kafka fault-detection analysis module.

Runs every ANALYSIS_INTERVAL_SECONDS (default: 10).  Each cycle:
  1. Computes window_start = now - 10 × ANALYSIS_INTERVAL_SECONDS.
  2. Finds the earliest scrape_id whose scraped_at >= window_start.
     If all data is within the window (started recently), this is just
     the first ever scrape_id — i.e. we load from the beginning.
  3. Loads all sessions, jmx_samples, and group_lag_samples from that
     scrape_id onward.
  4. Extracts features for each trained model (same logic as training).
  5. Runs inference against the latest scrape_id.
  6. Logs results to stdout and exposes them via /api/faults.

Volume mounts (already in docker-compose):
  ./data:/app/data      →  metrics.db written by the scraper container
  ./models:/app/models  →  trained XGBoost pickles
"""

import asyncio
import json
import logging
import os
import pickle
import sqlite3
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from ml.data import WINDOW
from ml.features import (
    extract_broker_saturation,
    extract_network_delay,
    extract_partition_skew,
    extract_rebalance_loops,
    extract_slow_consumer,
)
from ml.train import ENTITY_KEYS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("analyser")

# ── Configuration ──────────────────────────────────────────────────────────────

ANALYSIS_INTERVAL_SECONDS = int(os.getenv("ANALYSIS_INTERVAL_SECONDS", "10"))
DB_PATH = Path(os.getenv("DB_PATH", "data/metrics.db"))
MODELS_DIR = Path(os.getenv("MODELS_DIR", "models"))

WINDOW_SECONDS = 10 * ANALYSIS_INTERVAL_SECONDS

# ── App + shared state ─────────────────────────────────────────────────────────

app = FastAPI(title="Kafka Fault Analyser")

models: dict = {}
fault_history: deque = deque(maxlen=200)
current_faults: list = []

# ── Feature extractor dispatch ─────────────────────────────────────────────────

_EXTRACTORS = {
    "slow_consumer":      lambda jmx, lag, sess: extract_slow_consumer(jmx, lag),
    "rebalance_loops":    lambda jmx, lag, sess: extract_rebalance_loops(jmx, lag),
    "partition_skew":     lambda jmx, lag, sess: extract_partition_skew(jmx, lag),
    "broker_saturation":  lambda jmx, lag, sess: extract_broker_saturation(jmx, sess),
    "network_degradation": lambda jmx, lag, sess: extract_network_delay(jmx, sess),
}

# ── Data loading ───────────────────────────────────────────────────────────────


def _load_window_data(db_path: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame] | None:
    """
    Load scrapes covering the last WINDOW_SECONDS of data.

    Finds the earliest scrape_id whose scraped_at >= window_start, then
    fetches all rows from that id upward.  If the DB was started recently
    and all data falls within the window, the very first scrape_id is used
    (i.e. we load from the beginning).

    Returns (sessions, jmx, lag) or None when the DB has no usable data.
    """
    if not db_path.exists():
        return None

    window_start = (
        datetime.now(timezone.utc) - timedelta(seconds=WINDOW_SECONDS)
    ).isoformat()

    with sqlite3.connect(db_path) as conn:
        # Earliest scrape in the window.  If window_start predates all data
        # this returns the minimum id in the table (started-recently case).
        row = conn.execute(
            "SELECT MIN(id) FROM scrape_sessions WHERE scraped_at >= ?",
            (window_start,),
        ).fetchone()

        if row[0] is None:
            # No sessions at all (empty DB) — nothing to do yet.
            return None

        first_id: int = row[0]

        sessions = pd.read_sql(
            "SELECT id FROM scrape_sessions WHERE id >= ? ORDER BY id",
            conn,
            params=(first_id,),
        )
        if sessions.empty:
            return None

        scrape_ids = sessions["id"].tolist()
        placeholders = ",".join("?" * len(scrape_ids))

        jmx_raw = pd.read_sql(
            f"SELECT scrape_id, metric_name, value, labels "
            f"FROM jmx_samples WHERE scrape_id IN ({placeholders})",
            conn,
            params=scrape_ids,
        )
        lag = pd.read_sql(
            f"SELECT scrape_id, group_id, topic, partition, "
            f"committed_offset, log_end_offset, lag, group_state "
            f"FROM group_lag_samples WHERE scrape_id IN ({placeholders})",
            conn,
            params=scrape_ids,
        )

    if jmx_raw.empty and lag.empty:
        return None

    # Expand JSON labels → topic / partition / request columns
    if not jmx_raw.empty:
        parsed = jmx_raw["labels"].apply(json.loads)
        jmx = jmx_raw.drop(columns=["labels"]).copy()
        jmx["topic"]     = parsed.apply(lambda d: d.get("topic"))
        jmx["partition"] = parsed.apply(lambda d: d.get("partition"))
        jmx["request"]   = parsed.apply(lambda d: d.get("request"))
    else:
        jmx = pd.DataFrame(
            columns=["scrape_id", "metric_name", "value", "topic", "partition", "request"]
        )

    return sessions, jmx, lag


# ── Inference ──────────────────────────────────────────────────────────────────


def _run_inference(
    sessions: pd.DataFrame,
    jmx: pd.DataFrame,
    lag: pd.DataFrame,
) -> list[dict]:
    """
    For each loaded model:
      1. Extract features over the full loaded window (rolling features need
         prior context, so we pass all loaded scrapes).
      2. Filter to rows belonging to the latest scrape_id only.
      3. Predict and collect fault events.
    """
    latest_id = int(sessions["id"].max())
    events: list[dict] = []

    for fault_class, model in models.items():
        extractor = _EXTRACTORS.get(fault_class)
        if extractor is None:
            continue

        try:
            df = extractor(jmx, lag, sessions)
        except Exception as exc:
            logger.error("Feature extraction failed [%s]: %s", fault_class, exc)
            continue

        if df.empty:
            continue

        df_latest = df[df["scrape_id"] == latest_id].copy()
        if df_latest.empty:
            continue

        keys = ENTITY_KEYS.get(fault_class, ["scrape_id"])
        feature_cols = [c for c in df_latest.columns if c not in keys]
        if not feature_cols:
            continue

        try:
            X = df_latest[feature_cols].astype(float)
            preds = model.predict(X)
        except Exception as exc:
            logger.error("Prediction failed [%s]: %s", fault_class, exc)
            continue

        df_latest = df_latest.copy()
        df_latest["_pred"] = preds

        for _, row in df_latest[df_latest["_pred"] == 1].iterrows():
            entity: dict = {}
            if "group_id" in keys:
                entity["consumer_group"] = row.get("group_id", "unknown")
            if "topic" in keys:
                entity["topic"] = row.get("topic", "unknown")
            if fault_class == "broker_saturation":
                entity["scope"] = "broker"
            elif fault_class == "network_degradation":
                entity["scope"] = "global"

            event = {
                "scrape_id": latest_id,
                "fault_class": fault_class,
                "entity": entity,
                "features": {col: float(row[col]) for col in feature_cols},
                "detected_at": datetime.now(timezone.utc).isoformat(),
            }
            events.append(event)
            logger.warning("FAULT  %-22s | entity=%s", fault_class, entity)

    return events


# ── Main analysis loop ─────────────────────────────────────────────────────────


async def _analysis_loop() -> None:
    global current_faults, fault_history

    logger.info(
        "Analysis loop started — interval=%ds, data window=%ds",
        ANALYSIS_INTERVAL_SECONDS,
        WINDOW_SECONDS,
    )

    while True:
        await asyncio.sleep(ANALYSIS_INTERVAL_SECONDS)

        try:
            result = _load_window_data(DB_PATH)
            if result is None:
                logger.debug("No data yet, waiting…")
                continue

            sessions, jmx, lag = result
            latest_id = int(sessions["id"].max())
            logger.info(
                "Loaded %d scrapes (IDs %d–%d) — running inference…",
                len(sessions),
                int(sessions["id"].min()),
                latest_id,
            )

            events = _run_inference(sessions, jmx, lag)
            current_faults = events
            fault_history.extend(events)

            if not events:
                logger.info("No faults detected (scrape_id=%d)", latest_id)

        except Exception as exc:
            logger.error("Analysis cycle error: %s", exc, exc_info=True)


# ── Startup ────────────────────────────────────────────────────────────────────


@app.on_event("startup")
async def _startup() -> None:
    for pkl_path in MODELS_DIR.glob("*.pkl"):
        fault_class = pkl_path.stem
        try:
            with open(pkl_path, "rb") as f:
                payload = pickle.load(f)
            models[fault_class] = payload["model"] if isinstance(payload, dict) else payload
            logger.info("Loaded model: %s", fault_class)
        except Exception as exc:
            logger.error("Failed to load %s: %s", pkl_path, exc)

    if not models:
        logger.warning(
            "No models found in %s — inference will produce no detections.", MODELS_DIR
        )

    asyncio.create_task(_analysis_loop())


# ── REST API ───────────────────────────────────────────────────────────────────


@app.get("/api/faults")
async def get_faults():
    return {
        "current": current_faults,
        "history": list(fault_history),
    }


app.mount(
    "/static",
    StaticFiles(directory=Path(__file__).parent / "static"),
    name="static",
)


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(
        content=(Path(__file__).parent / "static" / "index.html").read_text()
    )
