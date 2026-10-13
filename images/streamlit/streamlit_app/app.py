"""Gold-layer dashboard for the de-labs Spark 4.2 study stack.

Reads the gold tables produced by notebooks/nb07_gold_and_serving.ipynb and
renders them with Plotly. Runs a plain local SparkSession (no cluster needed)
with the same Delta configuration the notebooks use.

Gold tables consumed:
    gold.orders_daily          order_date, revenue, orders, aov
    gold.revenue_by_region     region, revenue, orders
    gold.revenue_by_category    category, revenue, orders
    gold.top_products          product_id, name, category, revenue, units
    gold.order_status_funnel   status, orders
    gold.events_funnel         window_start, event_type, events   (from nb05, optional)
"""
from __future__ import annotations

import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from pyspark.sql import SparkSession

st.set_page_config(page_title="de-labs - Lakehouse Dashboard", layout="wide", initial_sidebar_state="expanded")

LAKE = "/data"
WAREHOUSE = f"{LAKE}/warehouse"

# name -> (display title, chart kind)
GOLD = {
    "orders_daily": ("Daily revenue", "line"),
    "revenue_by_region": ("Revenue by region", "bar"),
    "revenue_by_category": ("Revenue by category", "bar"),
    "top_products": ("Top products by revenue", "bar"),
    "order_status_funnel": ("Order status funnel", "bar"),
    "events_funnel": ("Streaming events per window", "line"),
}

BAR_COLORS = {
    "revenue_by_region": "region",
    "revenue_by_category": "category",
    "order_status_funnel": "status",
}


@st.cache_resource(show_spinner=False)
def get_spark() -> SparkSession:
    """One SparkSession for the app's lifetime (Delta configured explicitly:
    this container does not mount conf/spark-defaults.conf)."""
    return (
        SparkSession.builder.appName("de-labs-dashboard")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", WAREHOUSE)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def _read_one(spark: SparkSession, name: str) -> pd.DataFrame | None:
    """Read a gold table by catalog name, falling back to its on-disk location
    (the session catalog may not persist metadata across processes)."""
    try:
        return spark.table(f"gold.{name}").toPandas()
    except Exception:
        pass
    path = f"{WAREHOUSE}/gold.db/{name}"
    try:
        return spark.read.format("delta").load(path).toPandas()
    except Exception:
        return None


@st.cache_data(ttl=30, show_spinner=False)
def read_gold() -> dict[str, pd.DataFrame]:
    spark = get_spark()
    return {name: df for name in GOLD if (df := _read_one(spark, name)) is not None}


def _money(v: float) -> str:
    return f"{v:,.0f}"


# ---------------------------------------------------------------- sidebar
with st.sidebar:
    st.title("de-labs")
    st.caption("Apache Spark 4.2 + Delta Lake 4.4 study stack")
    if st.button("Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.caption(f"loaded {datetime.now():%H:%M:%S}")
    st.markdown(
        "**Study path**\n\n"
        "1. nb01 Spark 4 tour\n"
        "2. nb02 bronze to silver\n"
        "3. nb03 performance / AQE\n"
        "4. nb04 Delta Lake\n"
        "5. nb05 Structured Streaming\n"
        "6. nb06 Spark 4 feature lab\n"
        "7. nb07 gold + this dashboard"
    )

tables = read_gold()

if not tables:
    st.info(
        "No gold tables found yet.\n\n"
        "Run **notebooks/nb07_gold_and_serving.ipynb** in Jupyter "
        "(http://localhost:8888) to build them - this dashboard will pick them up "
        "on the next refresh. Seed the bronze layer first with `make seed` "
        "or see the README."
    )
    st.stop()

st.title("Lakehouse dashboard")

# ---------------------------------------------------------------- KPI row
kpis = {}
if "orders_daily" in tables:
    d = tables["orders_daily"]
    kpis = {
        "Total revenue": _money(d["revenue"].sum()),
        "Orders": f"{int(d['orders'].sum()):,}",
        "AOV": f"{d['revenue'].sum() / max(d['orders'].sum(), 1):,.2f}",
        "Days covered": str(d["order_date"].nunique()),
    }
elif "revenue_by_region" in tables:
    r = tables["revenue_by_region"]
    kpis = {
        "Total revenue": _money(r["revenue"].sum()),
        "Orders": f"{int(r['orders'].sum()):,}",
    }

if kpis:
    cols = st.columns(len(kpis))
    for col, (label, value) in zip(cols, kpis.items()):
        col.metric(label, value)

# ---------------------------------------------------------------- tabs
tab_labels = ["Overview", "Regions & categories", "Products", "Status", "Streaming"]
tabs = st.tabs(tab_labels)


def _chart(df: pd.DataFrame, x: str, y: str, color: str | None = None, title: str = ""):
    kind = px.bar if color else px.line
    fig = kind(df, x=x, y=y, color=color, color_discrete_sequence=px.colors.qualitative.Set2)
    fig.update_layout(margin=dict(l=10, r=10, t=30, b=10), title=title, showlegend=False if color else True)
    return fig


with tabs[0]:
    if "orders_daily" in tables:
        d = tables["orders_daily"].sort_values("order_date")
        st.plotly_chart(_chart(d, "order_date", "revenue", title="Daily revenue"), use_container_width=True)
        st.plotly_chart(_chart(d, "order_date", "orders", title="Daily order count"), use_container_width=True)
    else:
        st.caption("gold.orders_daily not built yet - run nb07.")

with tabs[1]:
    c1, c2 = st.columns(2)
    if "revenue_by_region" in tables:
        r = tables["revenue_by_region"].sort_values("revenue", ascending=True)
        c1.plotly_chart(_chart(r, "revenue", "region", color="region"), use_container_width=True)
    else:
        c1.caption("gold.revenue_by_region not built yet - run nb07.")
    if "revenue_by_category" in tables:
        cat = tables["revenue_by_category"].sort_values("revenue", ascending=True)
        c2.plotly_chart(_chart(cat, "revenue", "category", color="category"), use_container_width=True)
    else:
        c2.caption("gold.revenue_by_category not built yet - run nb07.")

with tabs[2]:
    if "top_products" in tables:
        t = tables["top_products"].nlargest(15, "revenue").sort_values("revenue")
        st.plotly_chart(_chart(t, "revenue", "name", color="category"), use_container_width=True)
        st.dataframe(
            tables["top_products"].head(25).reset_index(drop=True),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.caption("gold.top_products not built yet - run nb07.")

with tabs[3]:
    if "order_status_funnel" in tables:
        f = tables["order_status_funnel"].sort_values("orders", ascending=True)
        st.plotly_chart(_chart(f, "orders", "status", color="status"), use_container_width=True)
        st.dataframe(f.reset_index(drop=True), use_container_width=True, hide_index=True)
    else:
        st.caption("gold.order_status_funnel not built yet - run nb07.")

with tabs[4]:
    if "events_funnel" in tables:
        e = tables["events_funnel"].sort_values("window_start")
        fig = px.line(e, x="window_start", y="events", color="event_type")
        fig.update_layout(margin=dict(l=10, r=10, t=30, b=10))
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Source: gold.events_funnel written by nb05 Structured Streaming.")
    else:
        st.info("Streaming metrics not built yet - run **nb05**, then **nb07**.")
