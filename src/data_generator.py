"""
Synthetic Retail Price Data Generator
======================================

Generates 365 days of realistic daily pricing data for 20 electronics
products across 5 LATAM + global retailers.  The data mimics real
e-commerce patterns observed in Chilean retail markets:

* Base‑price variation per retailer (marketplace discount, department‑store markup)
* Weekly seasonality (weekend flash sales)
* Major promotional events (Cyber Day, Black Friday, Navidad, etc.)
* Random competitor price wars
* Gradual inflation drift (~4 % annual for CLP electronics)
* Stock‑out episodes
* Deliberately injected anomalies (pricing errors, flash crashes)

All prices are in Chilean Pesos (CLP).
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import List, Dict, Tuple

import numpy as np
import pandas as pd

from src.utils import (
    RETAILERS,
    RETAILER_PRICE_BIAS,
    PROMO_CALENDAR,
    SEED_PATH,
    DATA_DIR,
    get_logger,
)

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Load product catalog
# ---------------------------------------------------------------------------

def load_product_catalog() -> List[Dict]:
    """Load the seed product catalog from JSON."""
    with open(SEED_PATH, "r", encoding="utf-8") as fh:
        catalog = json.load(fh)
    logger.info("Loaded %d products from seed catalog", len(catalog))
    return catalog


# ---------------------------------------------------------------------------
# Core generator
# ---------------------------------------------------------------------------

def generate_price_series(
    base_price: float,
    n_days: int,
    retailer: str,
    rng: np.random.Generator,
    start_date: datetime,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Generate a single price time‑series for one product × one retailer.

    Returns
    -------
    prices : np.ndarray        – daily observed price
    original_prices : np.ndarray – "list" / reference price (before discount)
    in_stock : np.ndarray      – boolean availability
    is_promoted : np.ndarray   – boolean promotional flag
    """
    # Retailer‑specific base adjustment
    retailer_base = base_price * RETAILER_PRICE_BIAS.get(retailer, 1.0)
    # Small per‑retailer jitter so the same product isn't identical everywhere
    retailer_base *= rng.uniform(0.97, 1.03)

    prices = np.full(n_days, retailer_base, dtype=float)
    original_prices = np.full(n_days, retailer_base, dtype=float)
    in_stock = np.ones(n_days, dtype=bool)
    is_promoted = np.zeros(n_days, dtype=bool)

    dates = [start_date + timedelta(days=i) for i in range(n_days)]

    for i, dt in enumerate(dates):
        day_factor = 1.0

        # --- 1. Inflation drift (~4 % annual) ---
        day_factor += 0.04 * (i / 365)

        # --- 2. Daily micro‑noise (±0.8 %) ---
        day_factor *= rng.normal(1.0, 0.008)

        # --- 3. Weekly pattern: Fri‑Sun slight discount ---
        if dt.weekday() >= 4:  # Fri=4, Sat=5, Sun=6
            weekend_disc = rng.uniform(0.01, 0.04)
            day_factor *= (1 - weekend_disc)

        # --- 4. Promotional events ---
        for promo in PROMO_CALENDAR:
            if dt.month == promo["month"] and promo["day_start"] <= dt.day <= promo["day_end"]:
                # Not every retailer participates equally
                participation = rng.random()
                if participation > 0.2:  # 80 % chance retailer participates
                    disc = promo["discount"] * rng.uniform(0.7, 1.3)
                    day_factor *= (1 - disc)
                    is_promoted[i] = True
                break  # one promo at a time

        # --- 5. Random competitor price wars (≈3 % of days) ---
        if rng.random() < 0.03:
            war_drop = rng.uniform(0.05, 0.12)
            day_factor *= (1 - war_drop)

        # Compute final price, round to nearest 10 CLP
        raw_price = retailer_base * day_factor
        prices[i] = round(raw_price / 10) * 10

        # Original / "list" price stays near the inflation‑adjusted base
        original_prices[i] = round((retailer_base * (1 + 0.04 * (i / 365))) / 10) * 10

        # --- 6. Stock‑outs (≈5 % of days, clustered) ---
        if rng.random() < 0.02:
            out_len = rng.integers(1, 6)
            end_idx = min(i + out_len, n_days)
            in_stock[i:end_idx] = False

    # --- 7. Inject anomalies (pricing errors / flash crashes) ---
    n_anomalies = rng.integers(2, 6)
    anomaly_indices = rng.choice(n_days, size=n_anomalies, replace=False)
    for idx in anomaly_indices:
        anomaly_type = rng.choice(["crash", "spike"])
        if anomaly_type == "crash":
            prices[idx] *= rng.uniform(0.40, 0.65)  # 35‑60 % drop
        else:
            prices[idx] *= rng.uniform(1.30, 1.60)  # 30‑60 % spike
        prices[idx] = round(prices[idx] / 10) * 10

    return prices, original_prices, in_stock, is_promoted


# ---------------------------------------------------------------------------
# Main generation function
# ---------------------------------------------------------------------------

def generate_retail_data(
    n_days: int = 365,
    start_date: str = "2024-01-01",
    seed: int = 42,
) -> pd.DataFrame:
    """Generate the full synthetic retail pricing dataset.

    Parameters
    ----------
    n_days : int
        Number of calendar days to generate.
    start_date : str
        ISO date string for the first observation.
    seed : int
        Random seed for reproducibility.

    Returns
    -------
    pd.DataFrame
        Columns: date, product_id, product, brand, category, retailer,
                 price, original_price, discount_pct, in_stock, is_promoted
    """
    rng = np.random.default_rng(seed)
    catalog = load_product_catalog()
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    date_range = [start_dt + timedelta(days=i) for i in range(n_days)]

    records: List[Dict] = []

    for product in catalog:
        for retailer in RETAILERS:
            prices, orig, stock, promo = generate_price_series(
                base_price=product["base_price"],
                n_days=n_days,
                retailer=retailer,
                rng=rng,
                start_date=start_dt,
            )
            for i, dt in enumerate(date_range):
                disc_pct = max(0.0, 1 - prices[i] / orig[i]) if orig[i] > 0 else 0.0
                records.append(
                    {
                        "date": dt.strftime("%Y-%m-%d"),
                        "product_id": product["id"],
                        "product": product["name"],
                        "brand": product["brand"],
                        "category": product["category"],
                        "retailer": retailer,
                        "price": prices[i],
                        "original_price": orig[i],
                        "discount_pct": round(disc_pct, 4),
                        "in_stock": bool(stock[i]),
                        "is_promoted": bool(promo[i]),
                    }
                )

    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])
    logger.info(
        "Generated %s rows | %d products | %d retailers | %d days",
        f"{len(df):,}",
        len(catalog),
        len(RETAILERS),
        n_days,
    )
    return df


# ---------------------------------------------------------------------------
# CLI entry‑point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    data = generate_retail_data()
    out_path = os.path.join(DATA_DIR, "synthetic_prices.csv")
    data.to_csv(out_path, index=False)
    logger.info("Saved to %s", out_path)
