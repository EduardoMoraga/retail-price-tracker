"""
Retail Price Intelligence Tracker — Streamlit Application
============================================================

A production-grade dashboard for monitoring competitive retail pricing,
detecting anomalies, and generating actionable trade-marketing insights.

Run:
    streamlit run app.py
"""

from __future__ import annotations

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.etl_pipeline import run_pipeline, read_table
from src.anomaly_detection import detect_anomalies, anomaly_summary
from src.price_analysis import (
    price_leader_analysis,
    price_gap_matrix,
    price_vs_market_by_retailer,
    brand_price_index,
    promotional_effectiveness,
)
from src.report_generator import generate_insights, generate_kpi_cards
from src.utils import format_clp, pct_fmt, DB_PATH

# ===================================================================
# Page config & custom CSS
# ===================================================================

st.set_page_config(
    page_title="Retail Price Intelligence Tracker",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

CUSTOM_CSS = """
<style>
    /* Dark theme overrides */
    .stApp {
        background-color: #0f1419;
    }
    .main .block-container {
        padding-top: 2rem;
    }
    /* Accent color for headers */
    h1, h2, h3 {
        color: #00d4aa !important;
    }
    /* Metric cards */
    div[data-testid="stMetric"] {
        background-color: #1a2332;
        border: 1px solid #2a3a4a;
        border-radius: 10px;
        padding: 15px 20px;
    }
    div[data-testid="stMetric"] label {
        color: #8899aa !important;
    }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        color: #00d4aa !important;
    }
    /* Sidebar */
    section[data-testid="stSidebar"] {
        background-color: #111820;
    }
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #1a2332;
        border-radius: 8px;
        color: #8899aa;
        padding: 8px 16px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #00d4aa !important;
        color: #0f1419 !important;
    }
    /* Footer */
    .footer {
        text-align: center;
        padding: 20px 0;
        color: #556677;
        font-size: 0.85rem;
        border-top: 1px solid #2a3a4a;
        margin-top: 40px;
    }
    .footer a {
        color: #00d4aa;
        text-decoration: none;
    }
    /* Alert badges */
    .severity-high {
        background-color: #ff4444;
        color: white;
        padding: 2px 8px;
        border-radius: 4px;
        font-weight: bold;
    }
    .severity-medium {
        background-color: #ffaa00;
        color: #111;
        padding: 2px 8px;
        border-radius: 4px;
    }
    .severity-low {
        background-color: #44aaff;
        color: white;
        padding: 2px 8px;
        border-radius: 4px;
    }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

PLOTLY_TEMPLATE = "plotly_dark"
ACCENT = "#00d4aa"
COLORS = ["#00d4aa", "#ff6b6b", "#ffd93d", "#6bcbff", "#c084fc", "#ff9f43"]


# ===================================================================
# Data loading (cached)
# ===================================================================

@st.cache_data(show_spinner="Running ETL pipeline ...", ttl=3600)
def load_data():
    """Run pipeline and return all datasets."""
    stats = run_pipeline()
    df = read_table("price_metrics")
    alerts = read_table("alerts")
    catalog = read_table("product_catalog")
    try:
        log = read_table("pipeline_log")
    except Exception:
        log = pd.DataFrame()
    return df, alerts, catalog, log, stats


df, alerts, catalog, pipeline_log, pipeline_stats = load_data()


# ===================================================================
# Sidebar filters
# ===================================================================

st.sidebar.markdown("## Filters")

date_min = df["date"].min().date()
date_max = df["date"].max().date()
date_range = st.sidebar.date_input(
    "Date range",
    value=(date_min, date_max),
    min_value=date_min,
    max_value=date_max,
)

brands = st.sidebar.multiselect("Brands", sorted(df["brand"].unique()), default=sorted(df["brand"].unique()))
categories = st.sidebar.multiselect("Categories", sorted(df["category"].unique()), default=sorted(df["category"].unique()))
retailers = st.sidebar.multiselect("Retailers", sorted(df["retailer"].unique()), default=sorted(df["retailer"].unique()))

# Apply filters
mask = (
    (df["brand"].isin(brands))
    & (df["category"].isin(categories))
    & (df["retailer"].isin(retailers))
)
if len(date_range) == 2:
    mask &= (df["date"].dt.date >= date_range[0]) & (df["date"].dt.date <= date_range[1])

fdf = df[mask].copy()

# Filter alerts too
if not alerts.empty:
    alerts_mask = (
        alerts["brand"].isin(brands)
        & alerts["retailer"].isin(retailers)
    )
    if "date" in alerts.columns and len(date_range) == 2:
        alerts["date"] = pd.to_datetime(alerts["date"])
        alerts_mask &= (alerts["date"].dt.date >= date_range[0]) & (alerts["date"].dt.date <= date_range[1])
    f_alerts = alerts[alerts_mask].copy()
else:
    f_alerts = alerts.copy()


# ===================================================================
# Header
# ===================================================================

st.markdown("# Retail Price Intelligence Tracker")
st.markdown("*Real-time competitive pricing monitoring for LATAM electronics retail*")

# KPI row
kpis = generate_kpi_cards(fdf, f_alerts)
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Products Tracked", kpis["total_products"])
c2.metric("Retailers", kpis["total_retailers"])
c3.metric("Avg Promo Discount", pct_fmt(kpis["avg_discount"]))
c4.metric("Anomalies Detected", kpis["total_anomalies"])
c5.metric("Stock Availability", pct_fmt(kpis["stock_availability_rate"]))


# ===================================================================
# Tabs
# ===================================================================

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Pipeline Status",
    "Price Monitor",
    "Anomaly Alerts",
    "Competitive Intelligence",
    "Market Insights",
    "ETL Architecture",
])


# ------------------------------------------------------------------
# TAB 1 — Pipeline Status
# ------------------------------------------------------------------
with tab1:
    st.markdown("## ETL Pipeline Status")

    col_a, col_b, col_c, col_d = st.columns(4)
    col_a.metric("Status", "Healthy")
    col_b.metric("Rows Processed", f"{pipeline_stats.get('metrics_rows', 0):,}")
    col_c.metric("Duration", f"{pipeline_stats.get('duration_sec', 0):.1f}s")
    col_d.metric("Alerts Generated", f"{pipeline_stats.get('alerts_rows', 0):,}")

    st.markdown("### Table Row Counts")
    table_data = {
        "Table": ["prices", "price_metrics", "product_catalog", "alerts", "pipeline_log"],
        "Rows": [
            pipeline_stats.get("prices_rows", 0),
            pipeline_stats.get("metrics_rows", 0),
            pipeline_stats.get("catalog_rows", 0),
            pipeline_stats.get("alerts_rows", 0),
            len(pipeline_log) if not pipeline_log.empty else 1,
        ],
        "Status": ["OK"] * 5,
    }
    st.dataframe(pd.DataFrame(table_data), use_container_width=True, hide_index=True)

    st.markdown("### Data Freshness")
    st.info(f"Data covers **{kpis['date_range']}** | Generated with seed=42 for reproducibility")

    if not pipeline_log.empty:
        st.markdown("### Pipeline Run History")
        st.dataframe(pipeline_log.tail(10), use_container_width=True, hide_index=True)


# ------------------------------------------------------------------
# TAB 2 — Price Monitor
# ------------------------------------------------------------------
with tab2:
    st.markdown("## Price Monitor")

    # Category → Product cascading filter
    tab2_categories = sorted(fdf["category"].unique())
    selected_category = st.selectbox("Select a category", tab2_categories, index=0 if tab2_categories else None)

    if selected_category:
        products = sorted(fdf[fdf["category"] == selected_category]["product"].unique())
    else:
        products = sorted(fdf["product"].unique())

    selected_product = st.selectbox("Select a product", products, index=0 if products else None)

    if selected_product:
        pdf = fdf[fdf["product"] == selected_product].sort_values("date")

        # Price over time by retailer
        fig = px.line(
            pdf,
            x="date",
            y="price",
            color="retailer",
            title=f"Price Trend: {selected_product}",
            template=PLOTLY_TEMPLATE,
            color_discrete_sequence=COLORS,
            labels={"price": "Price (CLP)", "date": "Date"},
        )

        # Overlay anomalies
        if not f_alerts.empty:
            prod_alerts = f_alerts[f_alerts["product"] == selected_product]
            if not prod_alerts.empty:
                fig.add_trace(
                    go.Scatter(
                        x=prod_alerts["date"],
                        y=prod_alerts["price"],
                        mode="markers",
                        marker=dict(color="red", size=12, symbol="x"),
                        name="Anomaly",
                        hovertext=prod_alerts.get("anomaly_type", "anomaly"),
                    )
                )

        fig.update_layout(
            plot_bgcolor="#0f1419",
            paper_bgcolor="#0f1419",
            hovermode="x unified",
            legend=dict(orientation="h", y=-0.15),
            height=500,
        )
        st.plotly_chart(fig, use_container_width=True)

        # Current cheapest retailer
        latest_day = pdf["date"].max()
        latest_prices = pdf[pdf["date"] == latest_day][["retailer", "price", "in_stock"]].sort_values("price")
        cheapest = latest_prices[latest_prices["in_stock"] == True]
        if not cheapest.empty:
            st.success(
                f"**Cheapest available:** {cheapest.iloc[0]['retailer']} at "
                f"{format_clp(cheapest.iloc[0]['price'])}"
            )

        # Rolling averages
        fig2 = go.Figure()
        for ret in pdf["retailer"].unique():
            rdf = pdf[pdf["retailer"] == ret]
            fig2.add_trace(go.Scatter(
                x=rdf["date"], y=rdf["rolling_avg_7d"],
                mode="lines", name=f"{ret} (7d avg)",
                line=dict(dash="dash"),
            ))
        fig2.update_layout(
            title="7-Day Rolling Average by Retailer",
            template=PLOTLY_TEMPLATE,
            plot_bgcolor="#0f1419",
            paper_bgcolor="#0f1419",
            height=400,
            yaxis_title="Price (CLP)",
        )
        st.plotly_chart(fig2, use_container_width=True)

        # Price index
        fig3 = px.line(
            pdf, x="date", y="price_index", color="retailer",
            title="Price Index (Base 100 = First Day)",
            template=PLOTLY_TEMPLATE,
            color_discrete_sequence=COLORS,
        )
        fig3.update_layout(plot_bgcolor="#0f1419", paper_bgcolor="#0f1419", height=400)
        st.plotly_chart(fig3, use_container_width=True)


# ------------------------------------------------------------------
# TAB 3 — Anomaly Alerts
# ------------------------------------------------------------------
with tab3:
    st.markdown("## Anomaly Alerts")

    if f_alerts.empty:
        st.info("No anomalies detected in the filtered data.")
    else:
        # Summary cards
        ca, cb, cc = st.columns(3)
        ca.metric("Total Anomalies", len(f_alerts))
        cb.metric("High Severity", int((f_alerts["severity"] == "high").sum()))
        cc.metric("Unique Products Affected", f_alerts["product_id"].nunique())

        # Filter controls
        col1, col2 = st.columns(2)
        with col1:
            sev_filter = st.multiselect(
                "Severity", ["high", "medium", "low"],
                default=["high", "medium", "low"],
            )
        with col2:
            type_filter = st.multiselect(
                "Anomaly Type",
                f_alerts["anomaly_type"].unique().tolist() if "anomaly_type" in f_alerts.columns else [],
                default=f_alerts["anomaly_type"].unique().tolist() if "anomaly_type" in f_alerts.columns else [],
            )

        display_alerts = f_alerts[
            f_alerts["severity"].isin(sev_filter)
            & f_alerts["anomaly_type"].isin(type_filter)
        ].sort_values("date", ascending=False)

        st.dataframe(
            display_alerts[["date", "product", "brand", "retailer", "price",
                            "expected_price", "anomaly_type", "severity", "n_methods"]],
            use_container_width=True,
            hide_index=True,
            height=400,
        )

        # Anomaly trend
        if "date" in f_alerts.columns:
            trend = (
                f_alerts.groupby(f_alerts["date"].dt.to_period("W").astype(str))
                .size()
                .reset_index(name="count")
            )
            trend.columns = ["week", "count"]
            fig = px.bar(
                trend, x="week", y="count",
                title="Anomalies per Week",
                template=PLOTLY_TEMPLATE,
                color_discrete_sequence=[ACCENT],
            )
            fig.update_layout(plot_bgcolor="#0f1419", paper_bgcolor="#0f1419", height=350)
            st.plotly_chart(fig, use_container_width=True)

        # By retailer
        summ = anomaly_summary(f_alerts)
        if not summ["by_retailer"].empty:
            st.markdown("### Anomalies by Retailer")
            fig_r = px.bar(
                summ["by_retailer"].reset_index(),
                x="retailer",
                y=summ["by_retailer"].columns.tolist(),
                barmode="stack",
                template=PLOTLY_TEMPLATE,
                color_discrete_sequence=["#44aaff", "#ffaa00", "#ff4444"],
            )
            fig_r.update_layout(plot_bgcolor="#0f1419", paper_bgcolor="#0f1419", height=350)
            st.plotly_chart(fig_r, use_container_width=True)


# ------------------------------------------------------------------
# TAB 4 — Competitive Intelligence
# ------------------------------------------------------------------
with tab4:
    st.markdown("## Competitive Intelligence")

    # Category filter for this tab
    tab4_cat = st.selectbox("Filter by category", ["All Categories"] + sorted(fdf["category"].unique()), key="tab4_cat")
    if tab4_cat != "All Categories":
        fdf_ci = fdf[fdf["category"] == tab4_cat]
    else:
        fdf_ci = fdf

    # Price gap heatmap
    st.markdown(f"### Price vs Market Average (Retailer x Product) — {tab4_cat}")
    pvm = price_vs_market_by_retailer(fdf_ci)
    if not pvm.empty:
        heat_pivot = pvm.pivot_table(
            index="product", columns="retailer",
            values="avg_price_vs_market", aggfunc="mean",
        )
        fig_heat = px.imshow(
            heat_pivot.values,
            x=heat_pivot.columns.tolist(),
            y=heat_pivot.index.tolist(),
            color_continuous_scale=["#00d4aa", "#1a2332", "#ff4444"],
            aspect="auto",
            title="Price Gap Heatmap (green = below market, red = above)",
            template=PLOTLY_TEMPLATE,
            labels=dict(color="% vs Market"),
        )
        fig_heat.update_layout(
            plot_bgcolor="#0f1419", paper_bgcolor="#0f1419",
            height=600,
        )
        st.plotly_chart(fig_heat, use_container_width=True)

    # Price leader bar chart
    st.markdown("### Price Leadership by Brand")
    leaders = price_leader_analysis(fdf_ci)
    if not leaders.empty:
        fig_leader = px.bar(
            leaders,
            x="brand", y="pct_cheapest", color="retailer",
            barmode="group",
            title="% of Days as Cheapest Retailer (by Brand)",
            template=PLOTLY_TEMPLATE,
            color_discrete_sequence=COLORS,
            labels={"pct_cheapest": "% Days Cheapest"},
        )
        fig_leader.update_layout(plot_bgcolor="#0f1419", paper_bgcolor="#0f1419", height=450)
        st.plotly_chart(fig_leader, use_container_width=True)

    # Price gap matrix
    st.markdown("### Retailer vs Retailer Price Gap Matrix")
    gap_mx = price_gap_matrix(fdf_ci)
    if not gap_mx.empty:
        fig_mx = px.imshow(
            gap_mx.values.astype(float),
            x=gap_mx.columns.tolist(),
            y=gap_mx.index.tolist(),
            color_continuous_scale="RdYlGn_r",
            aspect="auto",
            title="Average Price Gap: Row vs Column (positive = Row is more expensive)",
            template=PLOTLY_TEMPLATE,
            labels=dict(color="Price Gap"),
        )
        fig_mx.update_layout(plot_bgcolor="#0f1419", paper_bgcolor="#0f1419", height=400)
        st.plotly_chart(fig_mx, use_container_width=True)

    # Promotional effectiveness
    st.markdown("### Promotional Effectiveness")
    promo_eff = promotional_effectiveness(fdf_ci)
    if not promo_eff["summary"].empty:
        st.dataframe(
            promo_eff["summary"][[
                "retailer", "avg_discount", "median_discount", "max_discount",
                "promo_frequency",
            ]].rename(columns={
                "avg_discount": "Avg Discount",
                "median_discount": "Median Discount",
                "max_discount": "Max Discount",
                "promo_frequency": "Promo Frequency",
            }),
            use_container_width=True,
            hide_index=True,
        )
    if not promo_eff["recovery"].empty:
        st.markdown("**Average Price Recovery Time After Promotions (days)**")
        st.dataframe(promo_eff["recovery"], use_container_width=True, hide_index=True)


# ------------------------------------------------------------------
# TAB 5 — Market Insights
# ------------------------------------------------------------------
with tab5:
    st.markdown("## Market Insights")

    insights = generate_insights(fdf, f_alerts)

    # Severity summary
    col_h, col_m, col_l = st.columns(3)
    col_h.metric("High Priority", sum(1 for i in insights if i["severity"] == "high"))
    col_m.metric("Medium Priority", sum(1 for i in insights if i["severity"] == "medium"))
    col_l.metric("Low Priority", sum(1 for i in insights if i["severity"] == "low"))

    # Insight cards
    for insight in insights:
        sev = insight["severity"]
        icon = {"high": "🔴", "medium": "🟡", "low": "🔵"}.get(sev, "⚪")
        cat_label = insight["category"].upper()
        st.markdown(
            f"**{icon} [{cat_label}]** {insight['text']}"
        )

    # Brand price index
    st.markdown("### Brand Price Index Trends")
    bpi = brand_price_index(fdf)
    if not bpi.empty:
        fig_bpi = px.line(
            bpi, x="date", y="avg_price_index", color="brand",
            title="Average Price Index by Brand (Base 100)",
            template=PLOTLY_TEMPLATE,
            color_discrete_sequence=COLORS,
            labels={"avg_price_index": "Price Index", "date": "Date"},
        )
        fig_bpi.update_layout(plot_bgcolor="#0f1419", paper_bgcolor="#0f1419", height=450)
        st.plotly_chart(fig_bpi, use_container_width=True)

    # Price volatility by brand
    st.markdown("### Price Volatility by Brand")
    vol = (
        fdf.groupby("brand")["price"]
        .agg(lambda s: s.std() / s.mean())
        .reset_index()
        .rename(columns={"price": "coefficient_of_variation"})
        .sort_values("coefficient_of_variation", ascending=False)
    )
    fig_vol = px.bar(
        vol, x="brand", y="coefficient_of_variation",
        title="Price Volatility (Coefficient of Variation)",
        template=PLOTLY_TEMPLATE,
        color_discrete_sequence=[ACCENT],
        labels={"coefficient_of_variation": "CV"},
    )
    fig_vol.update_layout(plot_bgcolor="#0f1419", paper_bgcolor="#0f1419", height=350)
    st.plotly_chart(fig_vol, use_container_width=True)

    # Discount distribution
    st.markdown("### Discount Distribution")
    if "discount_bucket" in fdf.columns:
        disc_dist = fdf["discount_bucket"].value_counts().reset_index()
        disc_dist.columns = ["bucket", "count"]
        fig_disc = px.bar(
            disc_dist, x="bucket", y="count",
            title="Discount Depth Distribution",
            template=PLOTLY_TEMPLATE,
            color_discrete_sequence=[ACCENT],
        )
        fig_disc.update_layout(plot_bgcolor="#0f1419", paper_bgcolor="#0f1419", height=350)
        st.plotly_chart(fig_disc, use_container_width=True)


# ------------------------------------------------------------------
# TAB 6 — ETL Architecture
# ------------------------------------------------------------------
with tab6:
    st.markdown("## ETL Pipeline Architecture")
    st.markdown(
        "The pipeline follows a classic **Extract - Transform - Load** pattern "
        "adapted for retail price intelligence at scale."
    )

    graph = """
    digraph ETL {
        rankdir=LR;
        bgcolor="#0f1419";
        node [
            shape=box,
            style="filled,rounded",
            fillcolor="#1a2332",
            fontcolor="#00d4aa",
            color="#2a3a4a",
            fontname="Helvetica"
        ];
        edge [color="#556677", fontcolor="#8899aa", fontname="Helvetica"];

        subgraph cluster_sources {
            label="Data Sources";
            fontcolor="#8899aa";
            color="#2a3a4a";
            style=dashed;
            Falabella; MercadoLibre; Paris; Ripley; Amazon;
        }

        subgraph cluster_etl {
            label="ETL Pipeline";
            fontcolor="#8899aa";
            color="#2a3a4a";
            style=dashed;

            Extract [label="EXTRACT\\nAPI / Scraping\\nSimulation"];
            Validate [label="VALIDATE\\nNulls, Dupes,\\nNegatives"];
            Enrich [label="ENRICH\\nRolling Avg,\\nPrice Index,\\nCompetitive Pos"];
            Load [label="LOAD\\nSQLite DB"];
        }

        subgraph cluster_analysis {
            label="Analysis Layer";
            fontcolor="#8899aa";
            color="#2a3a4a";
            style=dashed;

            Anomaly [label="ANOMALY\\nDETECTION\\n4 Methods +\\nEnsemble"];
            CompIntel [label="COMPETITIVE\\nINTELLIGENCE\\nPrice Gaps,\\nLeadership"];
            Insights [label="INSIGHT\\nGENERATOR\\nAuto Reports"];
        }

        subgraph cluster_output {
            label="Output";
            fontcolor="#8899aa";
            color="#2a3a4a";
            style=dashed;

            Dashboard [label="STREAMLIT\\nDASHBOARD", fillcolor="#00d4aa", fontcolor="#0f1419"];
            Alerts [label="PRICE\\nALERTS"];
        }

        Falabella -> Extract;
        MercadoLibre -> Extract;
        Paris -> Extract;
        Ripley -> Extract;
        Amazon -> Extract;

        Extract -> Validate -> Enrich -> Load;
        Load -> Anomaly;
        Load -> CompIntel;
        Load -> Insights;

        Anomaly -> Dashboard;
        Anomaly -> Alerts;
        CompIntel -> Dashboard;
        Insights -> Dashboard;
    }
    """
    st.graphviz_chart(graph)

    st.markdown("### Pipeline Components")
    st.markdown("""
| Component | Description | Technology |
|-----------|-------------|------------|
| **Data Generator** | Realistic synthetic price data with seasonal patterns | NumPy, Pandas |
| **ETL Pipeline** | Extract, validate, enrich, and load with full logging | SQLite, Pandas |
| **Anomaly Detection** | 4-method ensemble (Z-Score, IQR, Isolation Forest, RoC) | scikit-learn |
| **Price Analysis** | Competitive positioning, elasticity, promotional analysis | Pandas |
| **Report Generator** | Automated natural-language insights | Python |
| **Dashboard** | Interactive visualization with filtering | Streamlit, Plotly |
    """)

    st.markdown("### Database Schema")
    st.code("""
    price_tracker.db
    ├── prices          (raw daily prices: 36,500 rows)
    ├── price_metrics   (enriched with rolling avg, indices: 36,500 rows)
    ├── product_catalog (20 products × 5 brands)
    ├── alerts          (detected anomalies with severity)
    └── pipeline_log    (ETL run metadata: timestamp, rows, duration)
    """, language="text")


# ===================================================================
# Footer
# ===================================================================

st.markdown("---")
st.markdown(
    '<div class="footer">'
    'Built by <strong>Eduardo Moraga</strong> | '
    '<a href="https://eduardomoraga.github.io" target="_blank">eduardomoraga.github.io</a> | '
    'Trade Marketing &times; Data Science'
    '</div>',
    unsafe_allow_html=True,
)
