"""
Automated Insight & Report Generator
=======================================

Produces plain-language insights that a trade marketing manager can
immediately act on.  Each insight is a dict with:

    text     – human-readable sentence
    severity – low / medium / high
    category – pricing / anomaly / competitive / promotional
"""

from __future__ import annotations

from typing import List, Dict

import pandas as pd
import numpy as np

from src.utils import format_clp, pct_fmt, get_logger

logger = get_logger(__name__)


def _cheapest_retailer_insights(df: pd.DataFrame, n_days: int = 7) -> List[Dict]:
    """Compare current-week prices across retailers for each product."""
    insights: List[Dict] = []
    latest = df["date"].max()
    window = df[df["date"] > latest - pd.Timedelta(days=n_days)].copy()

    for pid, grp in window.groupby("product_id"):
        avg_by_ret = grp.groupby("retailer")["price"].mean().sort_values()
        if len(avg_by_ret) < 2:
            continue
        cheapest_ret = avg_by_ret.index[0]
        most_exp_ret = avg_by_ret.index[-1]
        gap = (avg_by_ret.iloc[-1] - avg_by_ret.iloc[0]) / avg_by_ret.iloc[0]
        if gap > 0.05:
            product_name = grp["product"].iloc[0]
            insights.append({
                "text": (
                    f"{product_name} is {pct_fmt(gap)} cheaper at {cheapest_ret} "
                    f"vs {most_exp_ret} this week"
                ),
                "severity": "medium" if gap > 0.10 else "low",
                "category": "competitive",
            })
    return insights


def _anomaly_insights(alerts: pd.DataFrame) -> List[Dict]:
    """Generate insights from recent anomaly alerts."""
    insights: List[Dict] = []
    if alerts.empty:
        return insights

    recent = alerts.sort_values("date", ascending=False).head(20)
    for _, row in recent.iterrows():
        direction = "dropped" if row.get("anomaly_type") == "price_drop" else "spiked"
        pct_change = abs(row["price"] - row["expected_price"]) / row["expected_price"]
        label = "potential pricing error" if pct_change > 0.25 else "unusual movement"
        insights.append({
            "text": (
                f"Alert: {row['product']} {direction} {pct_fmt(pct_change)} "
                f"at {row['retailer']} on {str(row['date'])[:10]} -- {label}"
            ),
            "severity": row.get("severity", "medium"),
            "category": "anomaly",
        })
    return insights


def _price_leader_insights(df: pd.DataFrame) -> List[Dict]:
    """Identify which retailer dominates pricing for each brand."""
    insights: List[Dict] = []
    df_stock = df[df["in_stock"] == True].copy()
    min_price = df_stock.groupby(["product_id", "date"])["price"].transform("min")
    df_stock["is_min"] = df_stock["price"] == min_price

    for brand, grp in df_stock.groupby("brand"):
        leader_counts = grp.groupby("retailer")["is_min"].sum()
        total = grp.groupby("retailer")["is_min"].count()
        pct_leader = (leader_counts / total).sort_values(ascending=False)
        top_ret = pct_leader.index[0]
        top_pct = pct_leader.iloc[0]
        if top_pct > 0.30:
            insights.append({
                "text": (
                    f"{top_ret} has been the price leader for {brand} products "
                    f"{pct_fmt(top_pct)} of the time this period"
                ),
                "severity": "low",
                "category": "competitive",
            })
    return insights


def _promotional_insights(df: pd.DataFrame) -> List[Dict]:
    """Insight on promotional intensity by retailer."""
    insights: List[Dict] = []
    promo = df[df["is_promoted"] == True]
    if promo.empty:
        return insights

    promo_rate = promo.groupby("retailer").size() / df.groupby("retailer").size()
    promo_rate = promo_rate.dropna().sort_values(ascending=False)

    if len(promo_rate) >= 2:
        top = promo_rate.index[0]
        bot = promo_rate.index[-1]
        insights.append({
            "text": (
                f"{top} runs promotions most aggressively ({pct_fmt(promo_rate.iloc[0])} of days), "
                f"while {bot} is the least promotional ({pct_fmt(promo_rate.iloc[-1])})"
            ),
            "severity": "low",
            "category": "promotional",
        })

    # Average discount depth
    avg_disc = promo.groupby("retailer")["discount_pct"].mean().sort_values(ascending=False)
    deepest = avg_disc.index[0]
    insights.append({
        "text": (
            f"{deepest} offers the deepest average promotional discount at "
            f"{pct_fmt(avg_disc.iloc[0])}"
        ),
        "severity": "low",
        "category": "promotional",
    })
    return insights


def _volatility_insights(df: pd.DataFrame) -> List[Dict]:
    """Flag brands/products with unusual price volatility."""
    insights: List[Dict] = []
    vol = (
        df.groupby(["brand", "product_id", "product"])["price"]
        .agg(lambda s: s.std() / s.mean())
        .reset_index()
        .rename(columns={"price": "cv"})
        .sort_values("cv", ascending=False)
    )
    top3 = vol.head(3)
    for _, row in top3.iterrows():
        if row["cv"] > 0.05:
            insights.append({
                "text": (
                    f"{row['product']} ({row['brand']}) shows high price volatility "
                    f"(CV = {row['cv']:.1%}) -- worth monitoring for pricing instability"
                ),
                "severity": "medium" if row["cv"] > 0.10 else "low",
                "category": "pricing",
            })
    return insights


# ===================================================================
# Public API
# ===================================================================

def generate_insights(
    df: pd.DataFrame,
    alerts: pd.DataFrame | None = None,
) -> List[Dict]:
    """Run all insight generators and return a consolidated list.

    Parameters
    ----------
    df : pd.DataFrame
        Enriched price metrics (output of transform step).
    alerts : pd.DataFrame, optional
        Anomaly alerts DataFrame.

    Returns
    -------
    list of dict
        Each dict has keys: text, severity, category.
    """
    insights: List[Dict] = []
    insights.extend(_cheapest_retailer_insights(df))
    insights.extend(_price_leader_insights(df))
    insights.extend(_promotional_insights(df))
    insights.extend(_volatility_insights(df))

    if alerts is not None and not alerts.empty:
        insights.extend(_anomaly_insights(alerts))

    # Sort by severity priority
    sev_order = {"high": 0, "medium": 1, "low": 2}
    insights.sort(key=lambda x: sev_order.get(x["severity"], 3))

    logger.info("REPORT | Generated %d insights", len(insights))
    return insights


def generate_kpi_cards(df: pd.DataFrame, alerts: pd.DataFrame | None = None) -> Dict:
    """Compute top-level KPI values for the dashboard header."""
    kpis = {
        "total_products": df["product_id"].nunique(),
        "total_retailers": df["retailer"].nunique(),
        "date_range": f"{df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}",
        "avg_discount": df[df["is_promoted"] == True]["discount_pct"].mean() if df["is_promoted"].any() else 0,
        "avg_price_volatility": (df.groupby("product_id")["price"].agg(lambda s: s.std() / s.mean()).mean()),
        "total_anomalies": len(alerts) if alerts is not None else 0,
        "high_severity_anomalies": (
            (alerts["severity"] == "high").sum() if alerts is not None and not alerts.empty else 0
        ),
        "stock_availability_rate": df["in_stock"].mean(),
    }
    return kpis
