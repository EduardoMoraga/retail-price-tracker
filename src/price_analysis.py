"""
Price Analysis & Competitive Intelligence
============================================

Business-oriented analytics that a trade marketing manager would actually use:

* **Price positioning** – which retailer is cheapest for each brand, and how often?
* **Price elasticity proxy** – correlation between price changes and stock availability.
* **Promotional effectiveness** – discount depth, frequency, recovery time.
* **Competitive intelligence** – price-gap matrices across retailers.
* **Brand analysis** – average price index evolution over time.
"""

from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd

from src.utils import RETAILERS, get_logger

logger = get_logger(__name__)


# ===================================================================
# Price Positioning
# ===================================================================

def price_leader_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """For each brand, count how often each retailer had the lowest price.

    Returns a DataFrame with columns: brand, retailer, days_cheapest,
    total_days, pct_cheapest.
    """
    # Keep only in-stock rows for fair comparison
    df_stock = df[df["in_stock"] == True].copy()

    # Flag cheapest per product per day
    min_price = df_stock.groupby(["product_id", "date"])["price"].transform("min")
    df_stock["is_min"] = df_stock["price"] == min_price

    result = (
        df_stock.groupby(["brand", "retailer"])
        .agg(days_cheapest=("is_min", "sum"), total_days=("is_min", "count"))
        .reset_index()
    )
    result["pct_cheapest"] = (result["days_cheapest"] / result["total_days"]).round(4)
    return result.sort_values(["brand", "pct_cheapest"], ascending=[True, False])


# ===================================================================
# Price Elasticity Proxy
# ===================================================================

def price_elasticity_proxy(df: pd.DataFrame) -> pd.DataFrame:
    """Estimate a rough demand proxy via stock-availability correlation.

    For each product-retailer pair, compute the Pearson correlation between
    the 7-day rolling price change and the 30-day stock-availability rate.
    A strong negative correlation suggests higher prices reduce demand
    (or trigger stock-outs sooner).
    """
    results = []
    for (pid, ret), grp in df.groupby(["product_id", "retailer"]):
        grp = grp.sort_values("date")
        if len(grp) < 30:
            continue
        price_chg = grp["price"].pct_change().rolling(7, min_periods=3).mean().dropna()
        stock = grp["in_stock"].astype(float).rolling(30, min_periods=7).mean()
        merged = pd.DataFrame({"price_chg": price_chg, "stock": stock}).dropna()
        if len(merged) < 20:
            continue
        corr = merged["price_chg"].corr(merged["stock"])
        results.append({
            "product_id": pid,
            "retailer": ret,
            "brand": grp["brand"].iloc[0],
            "product": grp["product"].iloc[0],
            "correlation": round(corr, 4) if not np.isnan(corr) else 0.0,
        })
    return pd.DataFrame(results).sort_values("correlation")


# ===================================================================
# Promotional Effectiveness
# ===================================================================

def promotional_effectiveness(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Analyse promotional depth, frequency, and price-recovery patterns.

    Returns a dict with:
      - summary : avg discount, promo frequency per retailer
      - recovery : average days for price to recover post-promo
    """
    promo = df[df["is_promoted"] == True].copy()

    # --- Summary by retailer ---
    summary = (
        promo.groupby("retailer")
        .agg(
            avg_discount=("discount_pct", "mean"),
            median_discount=("discount_pct", "median"),
            max_discount=("discount_pct", "max"),
            promo_days=("is_promoted", "count"),
        )
        .reset_index()
    )
    total_days_per_retailer = df.groupby("retailer")["date"].nunique().reset_index()
    total_days_per_retailer.columns = ["retailer", "total_days"]
    summary = summary.merge(total_days_per_retailer, on="retailer")
    summary["promo_frequency"] = (summary["promo_days"] / summary["total_days"]).round(4)

    # --- Price recovery: days until price returns to pre-promo level ---
    recovery_records = []
    for (pid, ret), grp in df.groupby(["product_id", "retailer"]):
        grp = grp.sort_values("date").reset_index(drop=True)
        promo_mask = grp["is_promoted"].values
        i = 0
        while i < len(grp):
            if promo_mask[i]:
                pre_price = grp["price"].iloc[max(0, i - 1)]
                # Find end of promo window
                j = i
                while j < len(grp) and promo_mask[j]:
                    j += 1
                # Now find recovery
                k = j
                while k < len(grp) and grp["price"].iloc[k] < pre_price * 0.98:
                    k += 1
                recovery_days = k - j if k < len(grp) else None
                recovery_records.append({
                    "product_id": pid,
                    "retailer": ret,
                    "recovery_days": recovery_days,
                })
                i = j
            else:
                i += 1

    recovery_df = pd.DataFrame(recovery_records)
    if not recovery_df.empty:
        recovery_agg = (
            recovery_df.dropna()
            .groupby("retailer")["recovery_days"]
            .agg(["mean", "median"])
            .reset_index()
            .rename(columns={"mean": "avg_recovery_days", "median": "med_recovery_days"})
        )
    else:
        recovery_agg = pd.DataFrame(columns=["retailer", "avg_recovery_days", "med_recovery_days"])

    return {"summary": summary, "recovery": recovery_agg}


# ===================================================================
# Competitive Intelligence – Price Gap Matrix
# ===================================================================

def price_gap_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Build a retailer x retailer price-gap matrix.

    Each cell (A, B) = average of (price_A - price_B) / price_B across all
    products and days where both retailers had stock.
    """
    pivot = df.pivot_table(
        index=["product_id", "date"],
        columns="retailer",
        values="price",
        aggfunc="first",
    )
    # Only rows where all retailers have data
    pivot = pivot.dropna()

    matrix = pd.DataFrame(index=RETAILERS, columns=RETAILERS, dtype=float)
    for ra in RETAILERS:
        for rb in RETAILERS:
            if ra == rb:
                matrix.loc[ra, rb] = 0.0
            elif ra in pivot.columns and rb in pivot.columns:
                gap = ((pivot[ra] - pivot[rb]) / pivot[rb]).mean()
                matrix.loc[ra, rb] = round(gap, 4)
            else:
                matrix.loc[ra, rb] = np.nan
    return matrix


def price_vs_market_by_retailer(df: pd.DataFrame) -> pd.DataFrame:
    """Average price-vs-market percentage for each retailer × product."""
    if "price_vs_market_pct" not in df.columns:
        market_avg = df.groupby(["product_id", "date"])["price"].transform("mean")
        df = df.copy()
        df["price_vs_market_pct"] = ((df["price"] / market_avg) - 1).round(4)

    return (
        df.groupby(["retailer", "product_id", "product", "brand", "category"])["price_vs_market_pct"]
        .mean()
        .reset_index()
        .rename(columns={"price_vs_market_pct": "avg_price_vs_market"})
        .sort_values("avg_price_vs_market")
    )


# ===================================================================
# Brand Analysis
# ===================================================================

def brand_price_index(df: pd.DataFrame) -> pd.DataFrame:
    """Average price index by brand over time."""
    if "price_index" not in df.columns:
        first_price = df.groupby(["product_id", "retailer"])["price"].transform("first")
        df = df.copy()
        df["price_index"] = ((df["price"] / first_price) * 100).round(2)

    return (
        df.groupby(["date", "brand"])["price_index"]
        .mean()
        .reset_index()
        .rename(columns={"price_index": "avg_price_index"})
    )
