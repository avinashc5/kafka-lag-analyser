#!/usr/bin/env python3
"""
Kafka fault demo: inject a fault mode, collect live metrics, run inference,
print a feature-importance-backed report.

Usage (from project root):
    python demo.py slow_consumer
    python demo.py rebalance_loops
    python demo.py partition_skew
    python demo.py broker_saturation
    python demo.py network_degradation

Options:
    --scrapes N      Collect N scrapes before running inference (default: 13).
                     Minimum is WINDOW (5); more gives rolling features better context.
    --keep-alive     Do not tear down the docker stack after analysis.
    --top N          Number of top features to show per detected fault (default: 5).
"""

import argparse
import atexit
import json
import os
import pickle
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from ml.data import WINDOW
from ml.features import (
    extract_broker_saturation,
    extract_network_delay,
    extract_partition_skew,
    extract_rebalance_loops,
    extract_slow_consumer,
)
from ml.train import ENTITY_KEYS

# ── Paths ──────────────────────────────────────────────────────────────────────

DB_PATH   = ROOT / "data" / "metrics.db"
MODELS_DIR = ROOT / "models"

# ── Fault configuration ────────────────────────────────────────────────────────
# Maps fault_mode → (CONSUMER_FAULT_MODE, PRODUCER_FAULT_MODE, description)
# Extra injection steps (CPU throttle, tc netem) are handled in main().

FAULT_CONFIG: dict[str, tuple[str, str, str]] = {
    "slow_consumer": (
        "slow_consumer", "healthy",
        "Consumer sleeps 2s per message → lag grows faster than offsets advance",
    ),
    "rebalance_loops": (
        "rebalance_loop", "healthy",
        "Consumer crashes after 20 msgs → Docker restarts it → continuous rebalancing",
    ),
    "partition_skew": (
        "healthy", "partition_imbalance",
        "Producer uses a fixed key → all writes land on partition 0",
    ),
    "broker_saturation": (
        "healthy", "high_throughput",
        "Producer floods messages + Kafka CPU capped at 5% → broker overload",
    ),
    "network_degradation": (
        "healthy", "healthy",
        "400ms delay added to Kafka NIC, 200ms to producer/consumer → high latency",
    ),
}

# ── ANSI colour helpers ────────────────────────────────────────────────────────

RED    = "\033[31m"
GREEN  = "\033[32m"
YELLOW = "\033[33m"
CYAN   = "\033[36m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
RESET  = "\033[0m"

def _c(code: str, text: str) -> str:
    return f"{code}{text}{RESET}"

def _ts() -> str:
    return time.strftime("%H:%M:%S")

def _info(msg: str)  -> None: print(f"{DIM}[{_ts()}]{RESET} {_c(CYAN,  msg)}")
def _warn(msg: str)  -> None: print(f"{DIM}[{_ts()}]{RESET} {_c(YELLOW, msg)}")
def _ok(msg: str)    -> None: print(f"{DIM}[{_ts()}]{RESET} {_c(GREEN,  msg)}")
def _err(msg: str)   -> None: print(f"{DIM}[{_ts()}]{RESET} {_c(RED,    msg)}", file=sys.stderr)

# ── Docker helpers ─────────────────────────────────────────────────────────────

def _run(cmd: list[str], env: dict | None = None, capture: bool = False) -> bool:
    kwargs: dict = dict(check=False)
    if capture:
        kwargs["stdout"] = subprocess.DEVNULL
        kwargs["stderr"] = subprocess.DEVNULL
    if env is not None:
        kwargs["env"] = env
    result = subprocess.run(cmd, **kwargs)
    return result.returncode == 0

def _compose(*args: str, env: dict | None = None, capture: bool = False) -> bool:
    return _run(["docker", "compose", *args], env=env, capture=capture)

def _exec_container(service: str, *cmd: str) -> bool:
    return _run(["docker", "compose", "exec", "-T", service, *cmd], capture=True)

# ── Stack lifecycle ────────────────────────────────────────────────────────────

_STACK_RUNNING = False

def teardown() -> None:
    global _STACK_RUNNING
    if not _STACK_RUNNING:
        return
    _info("Tearing down docker stack…")
    _compose("down", "--remove-orphans", capture=True)
    _STACK_RUNNING = False

def start_stack(consumer_fault: str, producer_fault: str) -> None:
    global _STACK_RUNNING

    _info("Stopping any previous run…")
    _compose("down", "--remove-orphans", capture=True)

    if DB_PATH.exists():
        DB_PATH.unlink()
    DB_PATH.parent.mkdir(exist_ok=True)

    env = {
        **os.environ,
        "CONSUMER_FAULT_MODE": consumer_fault,
        "PRODUCER_FAULT_MODE": producer_fault,
    }
    _info(
        f"Starting stack  "
        f"(CONSUMER_FAULT_MODE={consumer_fault}  PRODUCER_FAULT_MODE={producer_fault})…"
    )
    ok = _compose("up", "-d", "--build", env=env)
    if not ok:
        _err("docker compose up failed — is Docker running?")
        sys.exit(1)
    _STACK_RUNNING = True

def inject_broker_saturation() -> None:
    _warn("Throttling Kafka broker CPU to 5%…")
    ok = _run(["docker", "update", "--cpus=0.05", "kafka"], capture=True)
    if ok:
        _ok("  kafka container CPU capped at 0.05 cores")
    else:
        _warn("  docker update failed — broker saturation signals may be weaker")

def inject_network_degradation() -> None:
    _warn("Injecting network delays via tc netem…")
    targets = [
        ("kafka",    "400ms", "inter-broker / client path"),
        ("producer", "200ms", "producer → broker path"),
        ("consumer", "200ms", "broker → consumer path"),
    ]
    for service, delay, label in targets:
        ok = _exec_container(
            service, "tc", "qdisc", "add", "dev", "eth0", "root", "netem", "delay", delay
        )
        if ok:
            _ok(f"  {label}: +{delay}")
        else:
            _warn(
                f"  {label}: tc failed (container may need NET_ADMIN cap or iproute2 installed)"
            )

# ── Data collection ────────────────────────────────────────────────────────────

def _count_scrapes() -> int:
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute("SELECT COUNT(*) FROM scrape_sessions").fetchone()
            return row[0] if row else 0
    except Exception:
        return 0

def wait_for_scrapes(target: int, poll_s: int = 5, timeout_s: int = 600) -> None:
    _info(f"Waiting for {target} scrapes (≈{target * 10}s at 10s poll interval)…")
    deadline = time.time() + timeout_s
    while True:
        n = _count_scrapes()
        elapsed = int(time.time() - (deadline - timeout_s))
        print(f"  [{elapsed:>4}s] {n}/{target} scrapes", end="\r", flush=True)
        if n >= target:
            print()
            _ok(f"Collected {n} scrapes.")
            return
        if time.time() > deadline:
            print()
            _warn(f"Timeout reached with only {n}/{target} scrapes — continuing anyway.")
            return
        time.sleep(poll_s)

# ── Database loading ───────────────────────────────────────────────────────────

def load_all_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame] | None:
    if not DB_PATH.exists():
        return None
    with sqlite3.connect(DB_PATH) as conn:
        sessions = pd.read_sql("SELECT id FROM scrape_sessions ORDER BY id", conn)
        if sessions.empty:
            return None
        ids = sessions["id"].tolist()
        ph = ",".join("?" * len(ids))
        jmx_raw = pd.read_sql(
            f"SELECT scrape_id, metric_name, value, labels "
            f"FROM jmx_samples WHERE scrape_id IN ({ph})",
            conn, params=ids,
        )
        lag = pd.read_sql(
            f"SELECT scrape_id, group_id, topic, partition, "
            f"committed_offset, log_end_offset, lag, group_state "
            f"FROM group_lag_samples WHERE scrape_id IN ({ph})",
            conn, params=ids,
        )
    if jmx_raw.empty and lag.empty:
        return None
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

# ── Models ─────────────────────────────────────────────────────────────────────

def load_models() -> dict:
    models = {}
    for pkl in MODELS_DIR.glob("*.pkl"):
        try:
            with open(pkl, "rb") as f:
                payload = pickle.load(f)
            models[pkl.stem] = payload["model"] if isinstance(payload, dict) else payload
        except Exception as exc:
            _warn(f"Could not load {pkl.name}: {exc}")
    return models

# ── Inference ──────────────────────────────────────────────────────────────────

_EXTRACTORS = {
    "slow_consumer":       lambda jmx, lag, sess: extract_slow_consumer(jmx, lag),
    "rebalance_loops":     lambda jmx, lag, sess: extract_rebalance_loops(jmx, lag),
    "partition_skew":      lambda jmx, lag, sess: extract_partition_skew(jmx, lag),
    "broker_saturation":   lambda jmx, lag, sess: extract_broker_saturation(jmx, sess),
    "network_degradation": lambda jmx, lag, sess: extract_network_delay(jmx, sess),
}

def run_inference(
    sessions: pd.DataFrame,
    jmx: pd.DataFrame,
    lag: pd.DataFrame,
    models: dict,
) -> dict:
    """
    Run every loaded model against the latest scrape_id.

    Returns a dict:
        fault_class → {
            "detections": [{ entity, feature_vals, importances }, ...],
            "error": str | None,
        }
    """
    latest_id = int(sessions["id"].max())
    results: dict = {}

    for fault_class, model in models.items():
        extractor = _EXTRACTORS.get(fault_class)
        if extractor is None:
            continue

        try:
            df = extractor(jmx, lag, sessions)
        except Exception as exc:
            results[fault_class] = {"detections": [], "error": str(exc)}
            continue

        keys = ENTITY_KEYS.get(fault_class, ["scrape_id"])
        feature_cols = [c for c in df.columns if c not in keys]

        df_latest = df[df["scrape_id"] == latest_id].copy()
        if df_latest.empty or not feature_cols:
            results[fault_class] = {"detections": [], "error": None}
            continue

        try:
            X = df_latest[feature_cols].astype(float)
            preds = model.predict(X)
        except Exception as exc:
            results[fault_class] = {"detections": [], "error": str(exc)}
            continue

        df_latest = df_latest.copy()
        df_latest["_pred"] = preds

        # Feature names and importances from the trained model.
        # Use the model's own feature_names (set during XGBoost .fit on a
        # named DataFrame) so the order is guaranteed to match training.
        trained_feature_names = model.get_booster().feature_names or feature_cols
        importances: dict[str, float] = dict(
            zip(trained_feature_names, map(float, model.feature_importances_))
        )

        detections = []
        for _, row in df_latest[df_latest["_pred"] == 1].iterrows():
            entity: dict[str, str] = {}
            if "group_id" in keys:
                entity["consumer_group"] = str(row.get("group_id", "?"))
            if "topic" in keys:
                entity["topic"] = str(row.get("topic", "?"))
            if fault_class in ("broker_saturation", "network_degradation"):
                entity["scope"] = "global"

            detections.append({
                "entity": entity,
                "feature_vals": {col: float(row[col]) for col in feature_cols},
                "importances": importances,
            })

        results[fault_class] = {"detections": detections, "error": None}

    return results, latest_id

# ── Report ─────────────────────────────────────────────────────────────────────

_ORDERED_CLASSES = [
    "slow_consumer", "rebalance_loops", "partition_skew",
    "broker_saturation", "network_degradation",
]

def print_report(
    fault_mode: str,
    results: dict,
    latest_id: int,
    top_n: int,
) -> None:
    W = 68

    print()
    print("═" * W)
    print(f"  KAFKA FAULT ANALYSER — DEMO RESULTS")
    print(f"  Injected fault mode : {_c(BOLD, fault_mode)}")
    print(f"  Inference scrape_id : {latest_id}")
    print("═" * W)

    any_detected = False

    for fc in _ORDERED_CLASSES:
        if fc not in results:
            continue
        info       = results[fc]
        error      = info.get("error")
        detections = info.get("detections", [])

        if error:
            status = _c(YELLOW, f"! {fc:<22}")
            print(f"\n  {status}  ERROR: {error[:50]}")
            continue

        if not detections:
            print(f"\n  {_c(GREEN, '✓')}  {fc:<22}  {_c(GREEN, 'clean')}")
            continue

        any_detected = True
        print(f"\n  {_c(RED + BOLD, '✗')}  {fc:<22}  {_c(RED + BOLD, 'FAULT DETECTED')}")

        for det in detections:
            entity      = det["entity"]
            feat_vals   = det["feature_vals"]
            importances = det["importances"]

            if entity:
                entity_str = "  ".join(f"{k}={_c(BOLD, v)}" for k, v in entity.items())
                print(f"      Entity : {entity_str}")

            # Rank features by model importance and show top_n
            ranked = sorted(importances.items(), key=lambda kv: kv[1], reverse=True)[:top_n]

            col_feat = 38
            col_imp  = 11
            col_val  = 14
            hdr  = f"{'Feature':<{col_feat}}  {'Importance':>{col_imp}}  {'Observed value':>{col_val}}"
            rule = f"{'-' * col_feat}  {'-' * col_imp}  {'-' * col_val}"
            print(f"\n      Top {top_n} features driving this detection:")
            print(f"      {hdr}")
            print(f"      {rule}")

            for feat, imp in ranked:
                val = feat_vals.get(feat, float("nan"))
                # Highlight features with non-trivial values
                val_str = f"{val:>{col_val}.4f}"
                if val > 0:
                    val_str = _c(YELLOW, val_str)
                print(f"      {feat:<{col_feat}}  {imp:>{col_imp}.4f}  {val_str}")

    print()
    print("─" * W)
    expected_detected = fault_mode in [
        fc for fc, info in results.items() if info.get("detections")
    ]
    if expected_detected:
        print(
            f"  {_c(GREEN, '✓')} Expected fault {_c(BOLD, fault_mode)} was detected by its model."
        )
    else:
        print(
            f"  {_c(YELLOW, '!')} Expected fault {_c(BOLD, fault_mode)} was NOT detected "
            f"(false negative or not enough data)."
        )
    if any_detected:
        detected = [fc for fc, info in results.items() if info.get("detections")]
        print(f"  {_c(RED, 'Detected fault classes:')} {', '.join(detected)}")
    else:
        print(f"  {_c(GREEN, 'All models report clean.')}")
    print("═" * W)
    print()

# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inject a Kafka fault, collect live metrics, and run inference."
    )
    parser.add_argument(
        "fault_mode",
        choices=list(FAULT_CONFIG),
        metavar="fault_mode",
        help=f"One of: {', '.join(FAULT_CONFIG)}",
    )
    parser.add_argument(
        "--scrapes", type=int, default=WINDOW + 8,
        help="Number of scrapes to collect before inference (default: %(default)s)",
    )
    parser.add_argument(
        "--keep-alive", action="store_true",
        help="Leave the docker stack running after the demo",
    )
    parser.add_argument(
        "--top", type=int, default=5,
        help="Top N features to display per detected fault (default: %(default)s)",
    )
    args = parser.parse_args()

    consumer_fault, producer_fault, description = FAULT_CONFIG[args.fault_mode]

    # ── Print header ──────────────────────────────────────────────────────────

    W = 68
    print()
    print("═" * W)
    print(f"  KAFKA FAULT ANALYSER DEMO")
    print(f"  Fault mode : {_c(BOLD, args.fault_mode)}")
    print(f"  Scenario   : {description}")
    print("═" * W)
    print()

    # ── Load models (fail early) ──────────────────────────────────────────────

    _info("Loading trained models…")
    models = load_models()
    if not models:
        _err(f"No models found in {MODELS_DIR}. Run  python -m ml.train  first.")
        sys.exit(1)
    _ok(f"Loaded models: {', '.join(sorted(models))}")

    # ── Register cleanup ──────────────────────────────────────────────────────

    if not args.keep_alive:
        atexit.register(teardown)
        signal.signal(signal.SIGINT,  lambda *_: sys.exit(0))
        signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    # ── Bring up the stack ────────────────────────────────────────────────────

    start_stack(consumer_fault, producer_fault)

    # Extra injection for modes that need OS-level changes
    if args.fault_mode == "broker_saturation":
        _info("Waiting 20s for Kafka to start before throttling CPU…")
        time.sleep(20)
        inject_broker_saturation()

    elif args.fault_mode == "network_degradation":
        _info("Waiting 25s for all containers to be ready before adding delay…")
        time.sleep(25)
        inject_network_degradation()

    # ── Wait for enough scrapes ───────────────────────────────────────────────

    wait_for_scrapes(args.scrapes)

    # ── Load data and run inference ───────────────────────────────────────────

    _info("Loading collected data…")
    loaded = load_all_data()
    if loaded is None:
        _err("No data found in the database — the scraper may not have connected to Kafka.")
        sys.exit(1)
    sessions, jmx, lag = loaded

    _info(
        f"Running inference over {len(sessions)} scrapes "
        f"(IDs {int(sessions['id'].min())}–{int(sessions['id'].max())})…"
    )
    results, latest_id = run_inference(sessions, jmx, lag, models)

    # ── Print report ──────────────────────────────────────────────────────────

    print_report(args.fault_mode, results, latest_id, args.top)


if __name__ == "__main__":
    main()
