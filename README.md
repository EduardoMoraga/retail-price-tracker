# Retail Price Intelligence Tracker

[![Live Demo](https://img.shields.io/badge/Live_Demo-Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://retail-price-tracker-2zmycjminl99dbvp8sevpn.streamlit.app/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.30%2B-FF4B4B.svg)](https://streamlit.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

**[Live Demo](https://retail-price-tracker-2zmycjminl99dbvp8sevpn.streamlit.app/)**

**A production-grade competitive pricing intelligence platform for LATAM electronics retail, featuring automated ETL, multi-method anomaly detection, and actionable trade marketing insights.**

---

## Why This Matters

In trade marketing, **pricing is the most powerful lever you have** — and the hardest to monitor at scale.

A brand manager overseeing Samsung or LG across Falabella, MercadoLibre, Paris, Ripley, and Amazon needs to answer questions like:

- *"Is my product priced competitively across all channels right now?"*
- *"Did a retailer just break MAP (Minimum Advertised Price)?"*
- *"Which retailer is running the deepest promotions on my category?"*
- *"Are there pricing errors I should flag before they erode margin?"*

This project simulates that exact workflow using **realistic synthetic data** — the same patterns, seasonality, and anomalies you'd see in real e-commerce pricing, without the legal complications of actual web scraping.

> Built from 15+ years of trade marketing experience monitoring retail pricing across Latin America.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                             │
│  Falabella │ MercadoLibre │ Paris │ Ripley │ Amazon          │
└──────┬──────────┬───────────┬───────┬────────┬──────────────┘
       │          │           │       │        │
       ▼          ▼           ▼       ▼        ▼
┌─────────────────────────────────────────────────────────────┐
│                    ETL PIPELINE                              │
│  ┌──────────┐  ┌───────────┐  ┌──────────┐  ┌───────────┐  │
│  │ EXTRACT  │→ │ VALIDATE  │→ │  ENRICH  │→ │   LOAD    │  │
│  │ API/Web  │  │ Nulls,    │  │ Rolling  │  │  SQLite   │  │
│  │ Scraping │  │ Dupes,    │  │ Avg, Idx │  │  5 tables │  │
│  │ Sim.     │  │ Negatives │  │ Comp Pos │  │           │  │
│  └──────────┘  └───────────┘  └──────────┘  └───────────┘  │
└──────────────────────┬──────────────────────────────────────┘
                       │
       ┌───────────────┼───────────────┐
       ▼               ▼               ▼
┌────────────┐  ┌────────────┐  ┌────────────┐
│  ANOMALY   │  │ COMPETITIVE│  │  INSIGHT   │
│ DETECTION  │  │   INTEL    │  │ GENERATOR  │
│ 4 Methods  │  │ Price Gaps │  │ Auto       │
│ + Ensemble │  │ Leadership │  │ Reports    │
└─────┬──────┘  └─────┬──────┘  └─────┬──────┘
      │               │               │
      ▼               ▼               ▼
┌─────────────────────────────────────────────────────────────┐
│              STREAMLIT DASHBOARD                             │
│  Pipeline Status │ Price Monitor │ Anomaly Alerts            │
│  Competitive Intel │ Market Insights │ ETL Architecture      │
└─────────────────────────────────────────────────────────────┘
```

---

## Key Features

### Data Generation
- 20 electronics products across 5 global brands (Samsung, LG, Apple, Xiaomi, HP)
- 5 major LATAM + global retailers
- 365 days of daily pricing with realistic patterns:
  - Weekly seasonality (weekend sales)
  - Promotional events (Cyber Day, Black Friday, Navidad, etc.)
  - Competitor price wars
  - Inflation drift (~4% annual CLP)
  - Stock-out episodes
  - Deliberately injected anomalies

### ETL Pipeline
- Full extract-transform-load with validation and logging
- Rolling averages (7d, 30d), price indices, competitive positioning
- SQLite persistence with 5 normalized tables
- Pipeline health monitoring and run history

### Anomaly Detection (4-Method Ensemble)
| Method | Description |
|--------|-------------|
| Z-Score | Flags prices >2.5 std from 30-day rolling mean |
| IQR | Flags prices outside 1.5*IQR of recent window |
| Isolation Forest | Unsupervised ML on price features |
| Rate of Change | Flags single-day drops >15% |

An observation is flagged only when **2+ methods agree**, reducing false positives.

### Competitive Intelligence
- Price leadership analysis by brand and retailer
- Retailer vs retailer price gap matrices
- Promotional effectiveness (depth, frequency, recovery time)
- Price elasticity proxies

### Automated Insights
Natural-language alerts like:
- *"Samsung Galaxy S24 is 12% cheaper at MercadoLibre vs Falabella this week"*
- *"Alert: LG OLED evo 55" dropped 23% at Paris — potential pricing error"*
- *"Falabella has been the price leader for Apple products 67% of the time"*

---

## Screenshots

> *Run the app to see the interactive dashboard with dark theme, Plotly charts, and real-time filtering.*

| Tab | Description |
|-----|-------------|
| Pipeline Status | ETL health monitoring, row counts, run history |
| Price Monitor | Multi-retailer price trends with anomaly overlays |
| Anomaly Alerts | Severity-filtered anomaly table with trend charts |
| Competitive Intelligence | Heatmaps, price leadership, promotional analysis |
| Market Insights | Auto-generated insights, brand indices, volatility |
| ETL Architecture | Interactive pipeline diagram |

---

## Quick Start

### Prerequisites
- Python 3.10 or higher

### Installation

```bash
# Clone the repository
git clone https://github.com/eduardomoraga/retail-price-tracker.git
cd retail-price-tracker

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt
```

### Run the Dashboard

```bash
streamlit run app.py
```

The app will generate synthetic data, run the ETL pipeline, and launch the dashboard — all automatically on first load.

### Run the ETL Pipeline Standalone

```bash
python -m src.etl_pipeline
```

### Explore in Jupyter

```bash
jupyter notebook notebooks/price_intelligence.ipynb
```

---

## Tech Stack

| Layer | Technology |
|-------|------------|
| Data Generation | NumPy, Pandas |
| ETL & Storage | Pandas, SQLite |
| Anomaly Detection | scikit-learn (Isolation Forest), NumPy |
| Analysis | Pandas, NumPy |
| Visualization | Plotly (dark template) |
| Dashboard | Streamlit |
| Notebook | Jupyter |

---

## Business Value

This tool addresses real trade marketing challenges:

1. **Price Monitoring at Scale** — Track thousands of SKU-retailer combinations daily without manual checks.
2. **Anomaly Detection** — Catch pricing errors, unauthorized discounts, or competitor moves within hours, not days.
3. **Competitive Benchmarking** — Quantify exactly how your brand is positioned vs. competitors across every channel.
4. **Promotional ROI** — Measure discount depth, frequency, and price recovery to optimize trade spend.
5. **Executive Reporting** — Auto-generated insights replace hours of manual analysis.

---

## Project Structure

```
retail-price-tracker/
├── app.py                        # Streamlit dashboard (main entry point)
├── src/
│   ├── __init__.py
│   ├── data_generator.py         # Realistic synthetic retail price data
│   ├── etl_pipeline.py           # Full ETL: extract, transform, load
│   ├── anomaly_detection.py      # 4-method ensemble anomaly detection
│   ├── price_analysis.py         # Competitive intelligence & analytics
│   ├── report_generator.py       # Automated insight generation
│   └── utils.py                  # Shared constants, helpers, logging
├── data/
│   └── seed_products.json        # Product catalog (20 electronics SKUs)
├── notebooks/
│   └── price_intelligence.ipynb  # Full EDA and analysis notebook
├── outputs/                      # Generated reports and exports
├── requirements.txt
├── LICENSE
└── README.md
```

---

## Author

**Eduardo Moraga**
- Portfolio: [eduardomoraga.github.io](https://eduardomoraga.github.io)
- Background: 15+ years in Trade Marketing across LATAM
- Focus: Bridging commercial strategy with data science

---

## License

This project is licensed under the MIT License — see [LICENSE](LICENSE) for details.
