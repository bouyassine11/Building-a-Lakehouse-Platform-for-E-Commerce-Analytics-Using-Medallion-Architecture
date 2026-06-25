#!/usr/bin/env python3
"""Streamlit dashboard for the E-Commerce Lakehouse Gold layer."""

from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds
import streamlit as st

GOLD = Path(__file__).resolve().parent.parent / "data" / "gold"


@st.cache_data
def load_table(name):
    path = str(GOLD / name)
    return ds.dataset(path, format="parquet").to_table().to_pandas()


@st.cache_data
def load_fact_orders():
    path = str(GOLD / "fact_order")
    return (
        ds.dataset(path, format="parquet")
        .to_table(columns=["order_id", "customer_key", "product_key", "date_key", "total_amount", "order_status"])
        .to_pandas()
    )


@st.cache_data
def load_fact_payments():
    path = str(GOLD / "fact_payment")
    return (
        ds.dataset(path, format="parquet")
        .to_table(columns=["payment_id", "order_id", "amount", "payment_method", "payment_status"])
        .to_pandas()
    )


@st.cache_data
def load_fact_deliveries():
    path = str(GOLD / "fact_delivery")
    return (
        ds.dataset(path, format="parquet")
        .to_table(columns=["delivery_id", "order_id", "carrier", "estimated_days", "actual_days"])
        .to_pandas()
    )


df_customers = load_table("dim_customer")
df_products = load_table("dim_product")
df_dates = load_table("dim_date")
df_locations = load_table("dim_location")
df_orders = load_fact_orders()
df_payments = load_fact_payments()
df_deliveries = load_fact_deliveries()

df_customers_current = df_customers[df_customers["is_current"] == True]

st.set_page_config(page_title="E-Commerce Lakehouse", layout="wide")
st.title("E-Commerce Lakehouse Analytics")

with st.sidebar:
    st.header("Filters")

    years = sorted(df_dates["year"].unique())
    selected_years = st.multiselect("Year(s)", years, default=years, key="year_filter")

    categories = sorted(df_products["category"].unique())
    selected_categories = st.multiselect("Category(ies)", categories, default=categories, key="cat_filter")

    loyalty_tiers = sorted(df_customers_current["loyalty_tier"].dropna().unique())
    selected_tiers = st.multiselect("Loyalty Tier(s)", loyalty_tiers, default=loyalty_tiers, key="tier_filter")

    date_keys = df_dates[df_dates["year"].isin(selected_years)]["date_key"].tolist()
    category_products = df_products[df_products["category"].isin(selected_categories)]["product_key"].tolist()
    tier_customers = df_customers_current[df_customers_current["loyalty_tier"].isin(selected_tiers)]["customer_key"].tolist()

    df_orders_filtered = df_orders[
        df_orders["date_key"].isin(date_keys)
        & df_orders["product_key"].isin(category_products)
        & df_orders["customer_key"].isin(tier_customers)
    ]

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
with kpi1:
    total_revenue = df_orders_filtered["total_amount"].sum()
    st.metric("Total Revenue", f"${total_revenue:,.0f}")
with kpi2:
    total_orders = df_orders_filtered["order_id"].nunique()
    st.metric("Total Orders", f"{total_orders:,}")
with kpi3:
    avg_order = total_revenue / total_orders if total_orders else 0
    st.metric("Avg Order Value", f"${avg_order:,.2f}")
with kpi4:
    active_customers = df_orders_filtered["customer_key"].nunique()
    st.metric("Active Customers", f"{active_customers:,}")

tab1, tab2, tab3 = st.tabs(["Revenue", "Payments & Delivery", "Data Explorer"])

with tab1:
    col_a, col_b = st.columns(2)

    with col_a:
        top_customers = (
            df_orders_filtered
            .groupby("customer_key")["total_amount"]
            .sum()
            .reset_index()
            .merge(df_customers_current[["customer_key", "first_name", "last_name"]], on="customer_key")
            .assign(full_name=lambda x: x["first_name"] + " " + x["last_name"])
            .nlargest(10, "total_amount")
        )
        st.subheader("Top 10 Customers by Spend")
        st.bar_chart(top_customers.set_index("full_name")["total_amount"])

    with col_b:
        monthly = (
            df_orders_filtered
            .groupby("date_key")["total_amount"]
            .sum()
            .reset_index()
            .merge(df_dates[["date_key", "year", "month"]], on="date_key")
            .assign(ym=lambda x: x["year"].astype(str) + "-" + x["month"].astype(str).str.zfill(2))
        )
        st.subheader("Monthly Revenue Trend")
        st.line_chart(monthly.set_index("ym")["total_amount"])

    revenue_by_cat = (
        df_orders_filtered
        .groupby("product_key")["total_amount"]
        .sum()
        .reset_index()
        .merge(df_products[["product_key", "category"]], on="product_key")
        .groupby("category")["total_amount"]
        .sum()
        .reset_index()
        .sort_values("total_amount", ascending=False)
    )
    st.subheader("Revenue by Product Category")
    st.bar_chart(revenue_by_cat.set_index("category")["total_amount"])

with tab2:
    col_c, col_d = st.columns(2)

    with col_c:
        payment_by_method = (
            df_payments
            .merge(df_orders[["order_id", "date_key"]], on="order_id")
            .query("date_key in @date_keys")
            .groupby("payment_method")
            .agg(total=("payment_id", "count"), completed=("payment_status", lambda x: (x == "completed").sum()))
            .reset_index()
            .assign(success_pct=lambda x: round(x["completed"] / x["total"] * 100, 1))
            .sort_values("success_pct", ascending=False)
        )
        st.subheader("Payment Success Rate by Method")
        st.bar_chart(payment_by_method.set_index("payment_method")["success_pct"])

    with col_d:
        delivery_by_carrier = (
            df_deliveries
            .merge(df_orders[["order_id", "date_key"]], on="order_id")
            .query("date_key in @date_keys")
            .groupby("carrier")
            .agg(avg_estimated=("estimated_days", "mean"), avg_actual=("actual_days", "mean"))
            .reset_index()
            .assign(avg_delay=lambda x: round(x["avg_actual"] - x["avg_estimated"], 1))
            .sort_values("avg_delay")
        )
        st.subheader("Delivery Performance by Carrier")
        st.bar_chart(delivery_by_carrier.set_index("carrier")[["avg_estimated", "avg_actual"]])

with tab3:
    st.subheader("Gold Layer Tables")
    sel = st.selectbox("Select table", [
        "dim_customer", "dim_product", "dim_date", "dim_location",
        "fact_order", "fact_payment", "fact_delivery",
    ])
    n = st.slider("Rows to show", 10, 200, 50)
    if sel == "fact_order":
        st.dataframe(df_orders_filtered.head(n))
    elif sel == "fact_payment":
        st.dataframe(df_payments.head(n))
    elif sel == "fact_delivery":
        st.dataframe(df_deliveries.head(n))
    else:
        st.dataframe(load_table(sel).head(n))
