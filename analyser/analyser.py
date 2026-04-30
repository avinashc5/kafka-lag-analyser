import asyncio
import json
import logging
import sqlite3
from collections import deque
from pathlib import Path
import pickle

import pandas as pd
import uvicorn
import xgboost as xgb
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles

# We import the extraction logic from the ml directory.
from ml.data import WINDOW
from ml.features import (
    extract_broker_saturation,
    extract_network_delay,
    extract_partition_skew,
    extract_rebalance_loops,
    extract_slow_consumer,
)
from ml.train import ENTITY_KEYS

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(message)s")
logger = logging.getLogger("analyser")

app = FastAPI(title="Kafka Lag Analyser API")

# ── Paths ──────────────────────────────────────────────────────────────────────
# Assumes the analyser is run from the project root OR the container
# mounts the data and models directories in predictable locations.
DB_PATH = Path("data/metrics.db")
MODELS_DIR = Path("models")

# ── Global State ───────────────────────────────────────────────────────────────
models = {}
# Keep a rolling history of the last 100 detected faults
fault_history = deque(maxlen=100)
# Track the most recent fault state
current_faults = []
# Track last processed scrape_id so we don't re-process
last_processed_scrape_id = -1

# ── Database query helpers ─────────────────────────────────────────────────────

def load_recent_data(db_path: Path, last_n: int = WINDOW + 1) -> tuple:
    """
    Fetch the last `last_n` scrapes from the metrics DB.
    We need `WINDOW` recent scrapes so the rolling features (slopes, std) compute correctly.
    """
    if not db_path.exists():
        return None, pd.DataFrame(), pd.DataFrame()

    conn = sqlite3.connect(db_path)

    # Get recent sessions
    sessions = pd.read_sql(
        f"SELECT id FROM scrape_sessions ORDER BY id DESC LIMIT {last_n}",
        conn
    )
    if sessions.empty:
        conn.close()
        return None, pd.DataFrame(), pd.DataFrame()

    sessions = sessions.sort_values("id")  # sort chronologically
    scrape_ids = sessions["id"].tolist()
    scrape_ids_str = ",".join(map(str, scrape_ids))

    # Fetch related JMX and LAG data
    jmx_raw = pd.read_sql(
        f"SELECT scrape_id, metric_name, value, labels FROM jmx_samples "
        f"WHERE scrape_id IN ({scrape_ids_str})",
        conn,
    )
    lag = pd.read_sql(
        f"SELECT scrape_id, group_id, topic, partition, "
        f"committed_offset, log_end_offset, lag, group_state "
        f"FROM group_lag_samples WHERE scrape_id IN ({scrape_ids_str})",
        conn,
    )
    conn.close()

    if jmx_raw.empty and lag.empty:
        return None, pd.DataFrame(), pd.DataFrame()

    # Parse JSON labels (similar to ml.data)
    if not jmx_raw.empty:
        parsed = jmx_raw["labels"].apply(json.loads)
        jmx = jmx_raw.drop(columns=["labels"]).copy()
        jmx["topic"] = parsed.apply(lambda d: d.get("topic"))
        jmx["partition"] = parsed.apply(lambda d: d.get("partition"))
        jmx["request"] = parsed.apply(lambda d: d.get("request"))
    else:
        jmx = pd.DataFrame(columns=["scrape_id", "metric_name", "value", "topic", "partition", "request"])

    return sessions, jmx, lag


def _extract(fault_class: str, jmx: pd.DataFrame, lag: pd.DataFrame, sessions: pd.DataFrame) -> pd.DataFrame:
    """Wrapper matching the train.py extraction."""
    if fault_class == "slow_consumer":
        return extract_slow_consumer(jmx, lag)
    if fault_class == "rebalance_loops":
        return extract_rebalance_loops(jmx, lag)
    if fault_class == "partition_skew":
        return extract_partition_skew(jmx, lag)
    if fault_class == "broker_saturation":
        return extract_broker_saturation(jmx, sessions)
    if fault_class == "network_degradation":
        return extract_network_delay(jmx, sessions)
    raise ValueError(f"Unknown fault class: {fault_class!r}")


# ── Background Worker ──────────────────────────────────────────────────────────

async def inference_loop():
    """Poll pipeline evaluating new scrapes against loaded models."""
    global last_processed_scrape_id, current_faults, fault_history

    logger.info("Starting inference loop...")
    while True:
        try:
            if not DB_PATH.exists():
                await asyncio.sleep(5)
                continue

            # Quick check if there's a new scrape
            conn = sqlite3.connect(DB_PATH)
            cur = conn.execute("SELECT MAX(id) FROM scrape_sessions")
            row = cur.fetchone()
            conn.close()
            latest_id = row[0] if row else None

            if latest_id is None or latest_id <= last_processed_scrape_id:
                await asyncio.sleep(2)
                continue

            logger.info(f"Processing new scrape_id: {latest_id}")
            sessions, jmx, lag = load_recent_data(DB_PATH, last_n=WINDOW + 1)
            if sessions is None:
                await asyncio.sleep(2)
                continue

            new_faults = []

            for fault_class, model in models.items():
                try:
                    df = _extract(fault_class, jmx, lag, sessions)
                    if df.empty:
                        continue

                    # We only care about predicting for the most recent scrape
                    df_latest = df[df["scrape_id"] == latest_id].copy()
                    if df_latest.empty:
                        continue

                    # Separate Entity Keys vs Features
                    keys = ENTITY_KEYS.get(fault_class, ["scrape_id"])

                    feature_cols = [c for c in df_latest.columns if c not in keys and c != "fault"]
                    if not feature_cols:
                        continue

                    # Run prediction
                    X = df_latest[feature_cols].astype(float)
                    preds = model.predict(X)

                    df_latest["prediction"] = preds

                    # Filter rows where a fault was detected (prediction == 1)
                    detected = df_latest[df_latest["prediction"] == 1]

                    for _, row_data in detected.iterrows():
                        # Extract the key entity identifying the fault
                        entity = {}
                        if "group_id" in keys:
                            entity["consumer_group"] = row_data.get("group_id", "unknown")
                        if "topic" in keys:
                            entity["topic"] = row_data.get("topic", "unknown")

                        # Set default scope for global ones
                        if fault_class == "broker_saturation":
                            entity["broker"] = "broker-0"
                        if fault_class == "network_degradation":
                            entity["scope"] = "global"

                        fault_event = {
                            "scrape_id": latest_id,
                            "fault_class": fault_class,
                            "entity": entity,
                            "trigger_values": {col: float(row_data[col]) for col in feature_cols}
                        }
                        new_faults.append(fault_event)
                        fault_history.append(fault_event)
                        logger.warning(f"Detected FAULT: {fault_class} for {entity}")

                except Exception as e:
                    logger.error(f"Error evaluating {fault_class}: {e}")

            current_faults = new_faults
            last_processed_scrape_id = latest_id

        except Exception as e:
            logger.error(f"Inference loop error: {e}")

        await asyncio.sleep(2)


@app.on_event("startup")
async def startup_event():
    # Load all available .pkl models
    if MODELS_DIR.exists():
        for p in MODELS_DIR.glob("*.pkl"):
            fault_class = p.stem
            try:
                with open(p, "rb") as f:
                    payload = pickle.load(f)
                models[fault_class] = payload["model"] if isinstance(payload, dict) else payload
                logger.info(f"Loaded model for {fault_class}")
            except Exception as e:
                logger.error(f"Failed to load {fault_class} model: {e}")

    if not models:
        logger.warning(f"No trained models found in {MODELS_DIR}. Inference will run but detect nothing.")

    # Start the background task
    asyncio.create_task(inference_loop())


# ── API Endpoints ──────────────────────────────────────────────────────────────

@app.get("/api/faults")
async def get_faults():
    """
    Returns the currently active faults, as well as a short history of past faults.
    """
    return {
        "current": current_faults,
        "history": list(fault_history)
    }

# Mount the static directory
from fastapi.staticfiles import StaticFiles
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")

@app.get("/", response_class=HTMLResponse)
async def index():
    """Serve the static frontend UI"""
    frontend_path = Path(__file__).parent / "static" / "index.html"
    return HTMLResponse(content=frontend_path.read_text(), status_code=200)
