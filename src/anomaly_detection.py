"""
Price Anomaly Detection Module
================================

Four complementary detection methods plus an ensemble combiner:

1. **Z-Score** – flags prices > 2.5 std from a rolling 30-day mean.
2. **IQR** – flags prices outside 1.5 * IQR of a 30-day window.
3. **Isolation Forest** – sklearn unsupervised model on engineered features.
4. **Rate-of-Change** – flags single-day price drops > 15 %.

The *ensemble* marks a data point as a true anomaly when 2+ methods agree,
then assigns a severity (low / medium / high) based on deviation magnitude.
"""

from __future__ import annotations

from typing import List, Dict

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

from src.utils import SEVERITY_HIGH, SEVERITY_MEDIUM, SEVERITY_LOW, get_logger

logger = get_logger(__name__)

WINDOW = 30
Z_THRESHOLD = 2.5
IQR_FACTOR = 1.5
ROC_THRESHOLD = 0.15  # 15 % drop in a single day


# ===================================================================
# Individual detectors
# ===================================================================

def _zscore_detector(df: pd.DataFrame) -> pd.DataFrame:
    """Flag prices more than Z_THRESHOLD standard deviations from the rolling mean."""
    df = df.sort_values(["product_id", "retailer", "date"]).copy()
    grp = df.groupby(["product_id", "retailer"])["price"]

    roll_mean = grp.transform(lambda s: s.rolling(WINDOW, min_periods=7).mean())
    roll_std = grp.transform(lambda s: s.rolling(WINDOW, min_periods=7).std())

    z = ((df["price"] - roll_mean) / roll_std.replace(0, np.nan)).abs()
    mask = z > Z_THRESHOLD

    out = df.loc[mask, ["date", "product_id", "product", "brand", "retailer", "price"]].copy()
    out["expected_price"] = roll_mean[mask].round(0)
    out["anomaly_method"] = "zscore"
    out["deviation"] = z[mask].round(2)
    return out


def _iqr_detector(df: pd.DataFrame) -> pd.DataFrame:
    """Flag prices outside 1.5*IQR in a rolling window."""
    df = df.sort_values(["product_id", "retailer", "date"]).copy()
    grp = df.groupby(["product_id", "retailer"])["price"]

    q1 = grp.transform(lambda s: s.rolling(WINDOW, min_periods=7).quantile(0.25))
    q3 = grp.transform(lambda s: s.rolling(WINDOW, min_periods=7).quantile(0.75))
    iqr = q3 - q1
    lower = q1 - IQR_FACTOR * iqr
    upper = q3 + IQR_FACTOR * iqr

    mask = (df["price"] < lower) | (df["price"] > upper)

    out = df.loc[mask, ["date", "product_id", "product", "brand", "retailer", "price"]].copy()
    median = grp.transform(lambda s: s.rolling(WINDOW, min_periods=7).median())
    out["expected_price"] = median[mask].round(0)
    out["anomaly_method"] = "iqr"
    out["deviation"] = ((df["price"][mask] - median[mask]).abs() / median[mask].replace(0, np.nan)).round(4)
    return out


def _isolation_forest_detector(df: pd.DataFrame) -> pd.DataFrame:
    """Use Isolation Forest on price-level features per product."""
    results: List[pd.DataFrame] = []

    for (pid, ret), grp in df.groupby(["product_id", "retailer"]):
        grp = grp.sort_values("date").copy()
        if len(grp) < WINDOW:
            continue

        # Feature engineering
        grp["pct_change"] = grp["price"].pct_change().fillna(0)
        grp["roll_mean"] = grp["price"].rolling(WINDOW, min_periods=7).mean().bfill()
        grp["roll_std"] = grp["price"].rolling(WINDOW, min_periods=7).std().bfill().fillna(1)
        grp["price_ratio"] = grp["price"] / grp["roll_mean"]

        features = grp[["price", "pct_change", "roll_std", "price_ratio"]].fillna(0)

        clf = IsolationForest(
            n_estimators=100,
            contamination=0.02,
            random_state=42,
        )
        preds = clf.fit_predict(features)
        anomaly_mask = preds == -1

        if anomaly_mask.any():
            chunk = grp.loc[anomaly_mask, ["date", "product_id", "product", "brand", "retailer", "price"]].copy()
            chunk["expected_price"] = grp.loc[anomaly_mask, "roll_mean"].round(0)
            chunk["anomaly_method"] = "isolation_forest"
            chunk["deviation"] = (
                (grp.loc[anomaly_mask, "price"] - grp.loc[anomaly_mask, "roll_mean"]).abs()
                / grp.loc[anomaly_mask, "roll_mean"].replace(0, np.nan)
            ).round(4)
            results.append(chunk)

    if results:
        return pd.concat(results, ignore_index=True)
    return pd.DataFrame()


def _rate_of_change_detector(df: pd.DataFrame) -> pd.DataFrame:
    """Flag single-day price drops exceeding ROC_THRESHOLD."""
    df = df.sort_values(["product_id", "retailer", "date"]).copy()
    pct = df.groupby(["product_id", "retailer"])["price"].pct_change().fillna(0)
    mask = pct < -ROC_THRESHOLD

    out = df.loc[mask, ["date", "product_id", "product", "brand", "retailer", "price"]].copy()
    prev = df.groupby(["product_id", "retailer"])["price"].shift(1)
    out["expected_price"] = prev[mask].round(0)
    out["anomaly_method"] = "rate_of_change"
    out["deviation"] = pct[mask].abs().round(4)
    return out


# ===================================================================
# Ensemble
# ===================================================================

def _assign_severity(deviation: float) -> str:
    """Map deviation magnitude to severity."""
    if deviation >= 0.30:
        return SEVERITY_HIGH
    elif deviation >= 0.15:
        return SEVERITY_MEDIUM
    return SEVERITY_LOW


def detect_anomalies(df: pd.DataFrame, min_votes: int = 2) -> pd.DataFrame:
    """Run all detectors and return ensemble results.

    An observation is flagged as an anomaly if >= *min_votes* individual
    methods agree.  Returns a DataFrame with one row per anomaly event.
    """
    logger.info("ANOMALY | Running 4 detection methods on %s rows ...", f"{len(df):,}")

    parts = [
        _zscore_detector(df),
        _iqr_detector(df),
        _rate_of_change_detector(df),
        _isolation_forest_detector(df),
    ]

    combined = pd.concat([p for p in parts if not p.empty], ignore_index=True)

    if combined.empty:
        logger.info("ANOMALY | No anomalies detected")
        return pd.DataFrame(columns=[
            "date", "product_id", "product", "brand", "retailer",
            "price", "expected_price", "anomaly_type", "severity", "n_methods",
        ])

    # Count how many methods flagged each (date, product_id, retailer)
    key_cols = ["date", "product_id", "retailer"]
    votes = combined.groupby(key_cols).agg(
        n_methods=("anomaly_method", "nunique"),
        methods=("anomaly_method", lambda x: ", ".join(sorted(x.unique()))),
        price=("price", "first"),
        expected_price=("expected_price", "mean"),
        product=("product", "first"),
        brand=("brand", "first"),
        deviation=("deviation", "max"),
    ).reset_index()

    # Keep only those with enough votes
    ensemble = votes[votes["n_methods"] >= min_votes].copy()

    # Determine anomaly type from price vs expected
    ensemble["anomaly_type"] = np.where(
        ensemble["price"] < ensemble["expected_price"],
        "price_drop",
        "price_spike",
    )

    # Assign severity
    ensemble["severity"] = ensemble["deviation"].apply(_assign_severity)
    ensemble["expected_price"] = ensemble["expected_price"].round(0)

    ensemble = ensemble.sort_values(["date", "severity"], ascending=[False, True]).reset_index(drop=True)

    logger.info(
        "ANOMALY | Ensemble detected %d anomalies (high=%d, medium=%d, low=%d)",
        len(ensemble),
        (ensemble["severity"] == SEVERITY_HIGH).sum(),
        (ensemble["severity"] == SEVERITY_MEDIUM).sum(),
        (ensemble["severity"] == SEVERITY_LOW).sum(),
    )
    return ensemble


# ===================================================================
# Summary statistics
# ===================================================================

def anomaly_summary(alerts: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Return summary pivot tables for the dashboard."""
    if alerts.empty:
        return {"by_type": pd.DataFrame(), "by_retailer": pd.DataFrame(), "by_brand": pd.DataFrame()}
    return {
        "by_type": alerts.groupby("anomaly_type")["severity"].value_counts().unstack(fill_value=0),
        "by_retailer": alerts.groupby("retailer")["severity"].value_counts().unstack(fill_value=0),
        "by_brand": alerts.groupby("brand")["severity"].value_counts().unstack(fill_value=0),
    }
