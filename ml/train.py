"""
Training entry point for all fault classifiers.

Usage (from project root):
    python -m ml.train               # train all available fault classes
    python -m ml.train slow_consumer # train a specific fault class
"""

import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import classification_report
from sklearn.model_selection import StratifiedKFold
from sklearn.utils.class_weight import compute_sample_weight

from .data import FAULT_CLASSES, MODELS_DIR, TRAINING_DIR, load_fault_data
from .features import (
    extract_broker_saturation,
    extract_network_delay,
    extract_partition_skew,
    extract_rebalance_loops,
    extract_slow_consumer,
)

# ── Entity keys (columns that identify each training row, not features) ────────

ENTITY_KEYS: dict[str, list[str]] = {
    "slow_consumer":    ["scrape_id", "group_id"],
    "rebalance_loops":  ["scrape_id", "group_id"],
    "partition_skew":   ["scrape_id", "topic"],
    "broker_saturation": ["scrape_id"],
    "network_degradation":    ["scrape_id"],
}

# ── Feature extraction dispatch ────────────────────────────────────────────────


def _extract(fault_class: str, jmx: pd.DataFrame, lag: pd.DataFrame, sessions: pd.DataFrame) -> pd.DataFrame:
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


# ── Dataset builder ────────────────────────────────────────────────────────────


def build_dataset(fault_class: str) -> tuple[pd.DataFrame, pd.Series, list[str]]:
    """
    Returns (X, y, feature_names).
    Labels from labels.csv are joined on scrape_id so every entity row
    at a given scrape receives the fault label for that scrape.
    """
    sessions, jmx, lag, labels = load_fault_data(fault_class)
    features_df = _extract(fault_class, jmx, lag, sessions)

    dataset = features_df.merge(labels[["scrape_id", "label"]], on="scrape_id", how="inner")
    dataset = dataset.rename(columns={"label": "fault"})

    non_feature_cols = set(ENTITY_KEYS[fault_class] + ["fault"])
    feature_cols = [c for c in dataset.columns if c not in non_feature_cols]

    X = dataset[feature_cols].astype(float)
    y = dataset["fault"].astype(int)
    return X, y, feature_cols


# ── Model training ─────────────────────────────────────────────────────────────


def train_model(fault_class: str) -> xgb.XGBClassifier:
    print(f"\n{'━' * 55}")
    print(f"  {fault_class}")
    print(f"{'━' * 55}")

    X, y, feature_cols = build_dataset(fault_class)
    n_fault = int(y.sum())
    n_total = len(y)
    print(f"  Samples: {n_total}  |  Fault: {n_fault} ({n_fault / n_total:.1%})  |  Features: {len(feature_cols)}")

    if n_fault == 0 or n_fault == n_total:
        print("  ✗ Cannot train: labels are all one class.")
        raise ValueError(f"Degenerate labels for {fault_class!r}")

    sample_weights = compute_sample_weight("balanced", y)

    model = xgb.XGBClassifier(
        n_estimators=5,
        max_depth=3,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        eval_metric="logloss",
        random_state=42,
        verbosity=0,
    )

    # 5-fold stratified cross-validation
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    fold_f1s: list[float] = []

    for fold, (tr_idx, val_idx) in enumerate(skf.split(X, y), 1):
        X_tr, X_val = X.iloc[tr_idx], X.iloc[val_idx]
        y_tr, y_val = y.iloc[tr_idx], y.iloc[val_idx]
        w_tr = sample_weights[tr_idx]

        model.fit(X_tr, y_tr, sample_weight=w_tr)
        preds = model.predict(X_val)
        rep = classification_report(y_val, preds, output_dict=True, zero_division=0)
        f1 = rep.get("1", {}).get("f1-score", 0.0)
        fold_f1s.append(f1)
        print(f"  Fold {fold}  F1(fault)={f1:.3f}")

    mean_f1 = float(np.mean(fold_f1s))
    std_f1 = float(np.std(fold_f1s))
    print(f"  CV mean F1: {mean_f1:.3f} ± {std_f1:.3f}")

    # Final fit on full data
    model.fit(X, y, sample_weight=sample_weights)

    # Feature importance summary (top 5)
    importances = sorted(
        zip(feature_cols, model.feature_importances_),
        key=lambda t: t[1],
        reverse=True,
    )
    print("  Top features:")
    for name, imp in importances[:5]:
        print(f"    {name:<45} {imp:.4f}")

    return model


# ── Model persistence ──────────────────────────────────────────────────────────


def save_model(model: xgb.XGBClassifier, fault_class: str) -> Path:
    MODELS_DIR.mkdir(exist_ok=True)
    path = MODELS_DIR / f"{fault_class}.pkl"
    with open(path, "wb") as fh:
        pickle.dump(
            {"fault_class": fault_class, "model": model},
            fh,
            protocol=pickle.HIGHEST_PROTOCOL,
        )
    print(f"  Saved → {path}")
    return path


def load_model(fault_class: str) -> xgb.XGBClassifier:
    path = MODELS_DIR / f"{fault_class}.pkl"
    with open(path, "rb") as fh:
        payload = pickle.load(fh)
    return payload["model"]


# ── Main ───────────────────────────────────────────────────────────────────────


def _available_classes() -> list[str]:
    return [
        fc for fc in FAULT_CLASSES
        if (TRAINING_DIR / fc / "metrics.db").exists()
        and (TRAINING_DIR / fc / "labels.csv").exists()
    ]


def main(targets: list[str] | None = None) -> None:
    to_train = targets if targets else _available_classes()

    if not to_train:
        print(f"No training data found under {TRAINING_DIR}/.")
        print("Expected layout:")
        for fc in FAULT_CLASSES:
            print(f"  {TRAINING_DIR}/{fc}/metrics.db")
            print(f"  {TRAINING_DIR}/{fc}/labels.csv")
        sys.exit(1)

    print(f"Training: {', '.join(to_train)}")
    errors: list[str] = []

    for fault_class in to_train:
        try:
            model = train_model(fault_class)
            save_model(model, fault_class)
        except Exception as exc:
            print(f"  ✗ {fault_class} failed: {exc}")
            errors.append(fault_class)

    print(f"\n{'━' * 55}")
    succeeded = [fc for fc in to_train if fc not in errors]
    print(f"Done.  Trained: {len(succeeded)}/{len(to_train)}")
    if errors:
        print(f"Failed: {', '.join(errors)}")


if __name__ == "__main__":
    targets = sys.argv[1:] or None
    main(targets)
