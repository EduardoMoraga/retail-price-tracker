"""
ETL Pipeline for Retail Price Intelligence
============================================

Full Extract-Transform-Load pipeline that:

1. **Extract** – pulls raw data from the synthetic generator (simulating
   scraping / API ingestion).
2. **Transform** – validates, cleans, enriches with rolling averages,
   price indices, competitive-position flags, and discount-depth metrics.
3. **Load** – persists everything into a local SQLite database with five
   tables: prices, price_metrics, product_catalog, alerts, pipeline_log.

The pipeline is idempotent: re-running replaces previous data.
"""

from __future__ import annotations

import sqlite3
import time
from datetime import datetime
from typing import Dict, Any, Optional

import numpy as np
import pandas as pd

from src.data_generator import generate_retail_data, load_product_catalog
from src.utils import DB_PATH, ensure_dirs, get_logger

logger = get_logger(__name__)


# ===================================================================
# EXTRACT
# ===================================================================

def extract(
    n_days: int = 365,
    start_date: str = "2024-01-01",
    seed: int = 42,
) -> pd.DataFrame:
    """Extract raw pricing data from the synthetic data source.

    In production this would call a scraping service or API connector.
    """
    logger.info("EXTRACT | Generating synthetic data (%d days from %s)", n_days, start_date)
    df = generate_retail_data(n_days=n_days, start_date=start_date, seed=seed)
    return df


# ===================================================================
# TRANSFORM
# ===================================================================

def _validate(df: pd.DataFrame) -> pd.DataFrame:
    """Run data-quality checks and clean obvious problems."""
    initial_rows = len(df)

    # Drop exact duplicates
    df = df.drop_duplicates(subset=["date", "product_id", "retailer"])

    # Remove rows with null price or negative price
    df = df[df["price"].notna() & (df["price"] > 0)].copy()

    dropped = initial_rows - len(df)
    if dropped:
        logger.warning("VALIDATE | Dropped %d invalid/duplicate rows", dropped)
    else:
        logger.info("VALIDATE | All %s rows passed validation", f"{initial_rows:,}")
    return df


def _enrich(df: pd.DataFrame) -> pd.DataFrame:
    """Add calculated fields to the validated data."""
    df = df.sort_values(["product_id", "retailer", "date"]).copy()

    # --- Daily price change % ---
    df["price_change_pct"] = (
        df.groupby(["product_id", "retailer"])["price"]
        .pct_change()
        .fillna(0.0)
        .round(4)
    )

    # --- Rolling averages ---
    for window in (7, 30):
        col = f"rolling_avg_{window}d"
        df[col] = (
            df.groupby(["product_id", "retailer"])["price"]
            .transform(lambda s: s.rolling(window, min_periods=1).mean())
            .round(0)
        )

    # --- Price index (base 100 = first observation per product×retailer) ---
    first_price = df.groupby(["product_id", "retailer"])["price"].transform("first")
    df["price_index"] = ((df["price"] / first_price) * 100).round(2)

    # --- Cheapest retailer flag per product per day ---
    min_price = df.groupby(["product_id", "date"])["price"].transform("min")
    df["is_cheapest"] = df["price"] == min_price

    # --- Discount depth bucket ---
    df["discount_bucket"] = pd.cut(
        df["discount_pct"],
        bins=[-0.01, 0.0, 0.05, 0.10, 0.20, 0.35, 1.0],
        labels=["No discount", "0-5%", "5-10%", "10-20%", "20-35%", "35%+"],
    )

    # --- Stock availability rate (trailing 30-day window) ---
    df["stock_avail_30d"] = (
        df.groupby(["product_id", "retailer"])["in_stock"]
        .transform(lambda s: s.rolling(30, min_periods=1).mean())
        .round(3)
    )

    # --- Price gap vs market average ---
    market_avg = df.groupby(["product_id", "date"])["price"].transform("mean")
    df["price_vs_market_pct"] = ((df["price"] / market_avg) - 1).round(4)

    logger.info("ENRICH | Added %d calculated columns", 8)
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Full transform: validate then enrich."""
    logger.info("TRANSFORM | Starting on %s rows", f"{len(df):,}")
    df = _validate(df)
    df = _enrich(df)
    logger.info("TRANSFORM | Complete — %s rows, %d columns", f"{len(df):,}", len(df.columns))
    return df


# ===================================================================
# LOAD
# ===================================================================

def load(
    df_metrics: pd.DataFrame,
    alerts_df: Optional[pd.DataFrame] = None,
    db_path: str = DB_PATH,
) -> Dict[str, Any]:
    """Persist data into SQLite.

    Tables created / replaced:
        prices          – raw daily prices (subset of columns)
        price_metrics   – full enriched data
        product_catalog – product master
        alerts          – anomaly alerts (if provided)
        pipeline_log    – one row per ETL run
    """
    ensure_dirs()
    conn = sqlite3.connect(db_path)
    stats: Dict[str, Any] = {}

    try:
        # --- prices (raw) ---
        raw_cols = [
            "date", "product_id", "product", "brand", "category",
            "retailer", "price", "original_price", "discount_pct",
            "in_stock", "is_promoted",
        ]
        df_raw = df_metrics[raw_cols].copy()
        df_raw.to_sql("prices", conn, if_exists="replace", index=False)
        stats["prices_rows"] = len(df_raw)

        # --- price_metrics (enriched) ---
        df_metrics.to_sql("price_metrics", conn, if_exists="replace", index=False)
        stats["metrics_rows"] = len(df_metrics)

        # --- product_catalog ---
        catalog = load_product_catalog()
        pd.DataFrame(catalog).to_sql("product_catalog", conn, if_exists="replace", index=False)
        stats["catalog_rows"] = len(catalog)

        # --- alerts ---
        if alerts_df is not None and not alerts_df.empty:
            alerts_df.to_sql("alerts", conn, if_exists="replace", index=False)
            stats["alerts_rows"] = len(alerts_df)
        else:
            stats["alerts_rows"] = 0

        # --- pipeline_log ---
        log_entry = pd.DataFrame(
            [
                {
                    "run_id": datetime.utcnow().strftime("%Y%m%d%H%M%S"),
                    "timestamp": datetime.utcnow().isoformat(),
                    "rows_processed": len(df_metrics),
                    "errors": 0,
                    "status": "success",
                    "duration_sec": stats.get("duration_sec", 0),
                }
            ]
        )
        log_entry.to_sql("pipeline_log", conn, if_exists="append", index=False)

        conn.commit()
        logger.info(
            "LOAD | Written to %s — prices=%s, metrics=%s, alerts=%s",
            db_path,
            f"{stats['prices_rows']:,}",
            f"{stats['metrics_rows']:,}",
            f"{stats['alerts_rows']:,}",
        )
    finally:
        conn.close()

    return stats


# ===================================================================
# ORCHESTRATOR
# ===================================================================

def run_pipeline(
    n_days: int = 365,
    start_date: str = "2024-01-01",
    seed: int = 42,
    db_path: str = DB_PATH,
) -> Dict[str, Any]:
    """Execute the full ETL pipeline end-to-end.

    Returns a summary dict with row counts and timing.
    """
    t0 = time.time()

    # E
    raw = extract(n_days=n_days, start_date=start_date, seed=seed)

    # T
    enriched = transform(raw)

    # Anomaly detection (imported lazily to avoid circular import)
    from src.anomaly_detection import detect_anomalies
    alerts = detect_anomalies(enriched)

    # L
    duration = round(time.time() - t0, 2)
    stats = load(enriched, alerts_df=alerts, db_path=db_path)
    stats["duration_sec"] = duration
    stats["start_date"] = start_date
    stats["n_days"] = n_days

    logger.info("PIPELINE | Complete in %.1f s", duration)
    return stats


# ===================================================================
# Helper: read back from DB
# ===================================================================

def read_table(table: str, db_path: str = DB_PATH) -> pd.DataFrame:
    """Read a full table from the SQLite database."""
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql(f"SELECT * FROM {table}", conn)
    finally:
        conn.close()
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df


# ===================================================================
# CLI
# ===================================================================

if __name__ == "__main__":
    stats = run_pipeline()
    for k, v in stats.items():
        print(f"  {k}: {v}")
