"""
Kafka Fault Injector control panel — port 8001.

Serves a UI with sliders and radio buttons that write to data/control.json.
The producer and consumer read that file to adjust their behavior in real time.

Fault mappings:
  producer_rate high (>100/s)        → Slow Consumer (lag grows faster than consumed)
  message_size_bytes large (>10 KB)  → Broker Saturation (high bytes_in metric)
  num_partitions change >= 2         → Rebalance Loops (consumer_mode = rebalance_loop)
  partition_strategy = "uniform"     → Partition Skew (all writes to partition 0)
"""

import json
import os
from pathlib import Path

from fastapi import Body, FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
CONFIG_FILE = DATA_DIR / "control.json"

DEFAULT_CONFIG = {
    "producer_rate": 10,
    "num_partitions": 1,
    "message_size_bytes": 100,
    "partition_strategy": "hash",
    "consumer_mode": "healthy",
}

# Baseline partition count — tracks the last "stable" value so we can detect
# drastic changes even across restarts.
_baseline_partitions = 6

app = FastAPI(title="Kafka Control Panel")


def _read_config() -> dict:
    try:
        return json.loads(CONFIG_FILE.read_text())
    except Exception:
        return DEFAULT_CONFIG.copy()


def _write_config(config: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_FILE.write_text(json.dumps(config, indent=2))


@app.on_event("startup")
async def _startup() -> None:
    global _baseline_partitions
    if not CONFIG_FILE.exists():
        _write_config(DEFAULT_CONFIG.copy())
    else:
        cfg = _read_config()
        _baseline_partitions = cfg.get("num_partitions", 6)


@app.get("/api/control")
async def get_control():
    return _read_config()


@app.post("/api/control")
async def post_control(updates: dict = Body(...)):
    global _baseline_partitions
    current = _read_config()

    new_parts = int(updates.get("num_partitions", current.get("num_partitions", 6)))
    # Drastic partition change (>= 2 from baseline) → rebalance loops
    if abs(new_parts - _baseline_partitions) >= 2:
        updates["consumer_mode"] = "rebalance_loop"
    else:
        # Only reset consumer_mode if it was set by partition change
        if current.get("consumer_mode") == "rebalance_loop":
            updates["consumer_mode"] = "healthy"
        _baseline_partitions = new_parts

    current.update(updates)
    _write_config(current)
    return current


@app.post("/api/control/reset")
async def reset_control():
    global _baseline_partitions
    _baseline_partitions = DEFAULT_CONFIG["num_partitions"]
    _write_config(DEFAULT_CONFIG.copy())
    return DEFAULT_CONFIG.copy()


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
