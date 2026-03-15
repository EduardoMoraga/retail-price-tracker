"""
Utility functions for the Retail Price Intelligence Tracker.

Common helpers used across all modules: formatting, date handling,
configuration constants, and shared data structures.
"""

import os
import logging
from typing import List, Dict, Any

import pandas as pd
import numpy as np


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "outputs")
DB_PATH = os.path.join(DATA_DIR, "price_tracker.db")
SEED_PATH = os.path.join(DATA_DIR, "seed_products.json")


# ---------------------------------------------------------------------------
# Retail / Market Constants
# ---------------------------------------------------------------------------
RETAILERS: List[str] = ["Falabella", "MercadoLibre", "Paris", "Ripley", "Amazon"]

RETAILER_PRICE_BIAS: Dict[str, float] = {
    "Falabella": 1.00,
    "MercadoLibre": 0.96,
    "Paris": 1.02,
    "Ripley": 0.99,
    "Amazon": 0.94,
}

BRANDS: List[str] = ["Samsung", "LG", "Apple", "Xiaomi", "HP"]

CATEGORIES: List[str] = ["TV", "Smartphone", "Laptop", "Tablet", "Headphones"]

# Chilean‑market promotional calendar (month, day‑start, day‑end, label, avg discount)
PROMO_CALENDAR: List[Dict[str, Any]] = [
    {"month": 1, "day_start": 2, "day_end": 15, "label": "Liquidacion Verano", "discount": 0.15},
    {"month": 3, "day_start": 10, "day_end": 16, "label": "Dia del Consumidor", "discount": 0.12},
    {"month": 5, "day_start": 1, "day_end": 7, "label": "Hot Sale Mayo", "discount": 0.20},
    {"month": 6, "day_start": 15, "day_end": 21, "label": "Cyber Day", "discount": 0.25},
    {"month": 7, "day_start": 1, "day_end": 7, "label": "Mid‑Year Sale", "discount": 0.18},
    {"month": 9, "day_start": 1, "day_end": 7, "label": "Fiestas Patrias", "discount": 0.14},
    {"month": 10, "day_start": 1, "day_end": 7, "label": "Cyber Monday Oct", "discount": 0.22},
    {"month": 11, "day_start": 24, "day_end": 30, "label": "Black Friday / Cyber Monday", "discount": 0.28},
    {"month": 12, "day_start": 15, "day_end": 24, "label": "Navidad", "discount": 0.16},
]

# Severity levels
SEVERITY_HIGH = "high"
SEVERITY_MEDIUM = "medium"
SEVERITY_LOW = "low"


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def format_clp(value: float) -> str:
    """Format a number as Chilean Peso with thousands separator."""
    return f"${value:,.0f}".replace(",", ".")


def pct_fmt(value: float, decimals: int = 1) -> str:
    """Format a float as a percentage string."""
    return f"{value * 100:.{decimals}f}%"


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Division that returns *default* when denominator is zero or NaN."""
    if denominator == 0 or pd.isna(denominator):
        return default
    return numerator / denominator


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Return a consistently‑configured logger."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter(
            "[%(asctime)s] %(name)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def ensure_dirs() -> None:
    """Create data and output directories if they don't exist."""
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
