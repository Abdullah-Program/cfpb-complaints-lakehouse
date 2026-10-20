"""CFPB Consumer Complaints Lakehouse - Interactive BI & Observability Dashboard.

Connects to Snowflake (or local DuckDB fallback) to visualize:
    1. Executive KPIs (Total complaints, timely rate, relief rate, resolved entities)
    2. Interactive US Choropleth Heatmap (Complaints by State)
    3. Bank Resolution League Table (Timely response % vs Relief %)
    4. Product Issue Trends (Time-series analysis)
    5. Data Quality Scorecard (Missing state %, masked ZIP %, narrative consent)
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

load_dotenv(dotenv_path=PROJECT_ROOT / ".env")

# Page Configuration
st.set_page_config(
    page_title="CFPB Lakehouse Analytics",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Styling
st.markdown(
    """
    <style>
    .main-header { font-size: 2.2rem; font-weight: 700; color: #1E3A8A; margin-bottom: 0.2rem; }
    .sub-header { font-size: 1.1rem; color: #4B5563; margin-bottom: 1.5rem; }
    .metric-card { background-color: #F3F4F6; border-radius: 8px; padding: 15px; border-left: 5px solid #3B82F6; }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource
def get_connection(engine: str):
    """Establishes connection to Snowflake or local DuckDB."""
    if engine == "Snowflake":
        from pipeline.load_to_snowflake import get_snowflake_conn

        conn = get_snowflake_conn()
        return conn
    else:
        import duckdb

        duckdb_path = os.getenv("DBT_DUCKDB_PATH", str(PROJECT_ROOT / "data/warehouse/cfpb.duckdb"))
        return duckdb.connect(duckdb_path, read_only=True)


@st.cache_data(ttl=600)
def run_query(_conn, query: str, engine: str) -> pd.DataFrame:
    """Runs a query with caching for high performance."""
    if engine == "Snowflake":
        cur = _conn.cursor()
        cur.execute(query)
        df = cur.fetch_pandas_all()
        cur.close()
        df.columns = [c.upper() for c in df.columns]
        return df
    else:
        df = _conn.execute(query).df()
        df.columns = [c.upper() for c in df.columns]
        return df


def main():
    # Sidebar
    st.sidebar.markdown("## 🏛️ CFPB Analytics")
    st.sidebar.markdown("### ⚙️ Pipeline Configuration")

    engine = st.sidebar.radio("Warehouse Engine", ["Snowflake", "DuckDB (Local)"], index=0)
    st.sidebar.info(f"Connected to: **{engine}**")

    try:
        conn = get_connection(engine)
    except Exception as e:
        st.error(f"Failed to connect to {engine}: {e}")
        st.stop()

    st.markdown('<div class="main-header">🏦 CFPB Consumer Complaints Lakehouse</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="sub-header">Production Analytics & Data Observability Dashboard | Powered by Spark, dbt & Snowflake</div>',
        unsafe_allow_html=True,
    )

    # Database Schema prefix
    db_prefix = "CFPB.ANALYTICS." if engine == "Snowflake" else "analytics."
    marts_prefix = "CFPB.MARTS." if engine == "Snowflake" else "marts."

    # --------------------------------------------------------------------------
    # 1. Executive KPIs
    # --------------------------------------------------------------------------
    kpi_query = f"""
        SELECT 
            COUNT(*) AS total_complaints,
            COUNT(DISTINCT company_sk) AS distinct_companies,
            COUNT(DISTINCT product_sk) AS distinct_products,
            ROUND(AVG(CASE WHEN timely_response THEN 1.0 ELSE 0.0 END) * 100, 1) AS timely_rate_pct,
            ROUND(AVG(CASE WHEN is_relief THEN 1.0 ELSE 0.0 END) * 100, 1) AS relief_rate_pct
        FROM {marts_prefix}FACT_COMPLAINTS
    """
    df_kpi = run_query(conn, kpi_query, engine)

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Complaints", f"{int(df_kpi['TOTAL_COMPLAINTS'][0]):,}")
    with col2:
        st.metric("Resolved Companies", f"{int(df_kpi['DISTINCT_COMPANIES'][0])}")
    with col3:
        st.metric("Product Lines", f"{int(df_kpi['DISTINCT_PRODUCTS'][0])}")
    with col4:
        st.metric("Timely Response %", f"{df_kpi['TIMELY_RATE_PCT'][0]}%")
    with col5:
        st.metric("Monetary Relief %", f"{df_kpi['RELIEF_RATE_PCT'][0]}%")

    st.markdown("---")

    # --------------------------------------------------------------------------
    # Tabs
    # --------------------------------------------------------------------------
    tab_geo, tab_banks, tab_trends, tab_dq = st.tabs([
        "🗺️ Geographic Heatmap",
        "🏦 Company Resolution Performance",
        "📈 Product & Issue Trends",
        "🛡️ Data Quality Scorecard",
    ])

    # --------------------------------------------------------------------------
    # TAB 1: US Choropleth Map
    # --------------------------------------------------------------------------
    with tab_geo:
        st.subheader("Geographic Distribution of Complaints across US States")
        geo_query = f"""
            SELECT 
                state_code,
                state_name,
                region,
                SUM(complaints) AS total_complaints,
                ROUND(AVG(share_of_month) * 100, 2) AS avg_share_pct
            FROM {db_prefix}MART_GEO_COMPLAINTS
            WHERE state_code NOT IN ('Unknown', 'None', '')
            GROUP BY state_code, state_name, region
        """
        df_geo = run_query(conn, geo_query, engine)

        fig_map = px.choropleth(
            df_geo,
            locations="STATE_CODE",
            locationmode="USA-states",
            color="TOTAL_COMPLAINTS",
            scope="usa",
            color_continuous_scale="Blues",
            hover_name="STATE_NAME",
            hover_data={"STATE_CODE": True, "TOTAL_COMPLAINTS": ":,", "REGION": True, "AVG_SHARE_PCT": ":.2f%"},
            labels={"TOTAL_COMPLAINTS": "Total Complaints"},
        )
        fig_map.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, height=500)
        st.plotly_chart(fig_map, use_container_width=True)

    # --------------------------------------------------------------------------
    # TAB 2: Company Resolution Performance
    # --------------------------------------------------------------------------
    with tab_banks:
        st.subheader("Top Financial Institutions: Dispute vs Relief Rates")
        comp_query = f"""
            SELECT 
                company_name_clean,
                company_category,
                SUM(complaints) AS total_complaints,
                ROUND(AVG(timely_rate) * 100, 1) AS timely_rate_pct,
                ROUND(AVG(relief_rate) * 100, 1) AS relief_rate_pct,
                ROUND(AVG(dispute_rate) * 100, 1) AS dispute_rate_pct,
                ROUND(AVG(avg_days_to_company), 1) AS avg_days_to_company
            FROM {db_prefix}MART_COMPLAINTS_BY_COMPANY_MONTH
            WHERE company_name_clean != 'Unknown company'
            GROUP BY company_name_clean, company_category
            ORDER BY total_complaints DESC
            LIMIT 15
        """
        df_comp = run_query(conn, comp_query, engine)

        col_c1, col_c2 = st.columns(2)
        with col_c1:
            fig_bar = px.bar(
                df_comp,
                x="TOTAL_COMPLAINTS",
                y="COMPANY_NAME_CLEAN",
                orientation="h",
                title="Top 15 Most Complained-About Institutions",
                color="COMPANY_CATEGORY",
            )
            fig_bar.update_layout(yaxis={"categoryorder": "total ascending"}, height=500)
            st.plotly_chart(fig_bar, use_container_width=True)

        with col_c2:
            fig_scatter = px.scatter(
                df_comp,
                x="RELIEF_RATE_PCT",
                y="DISPUTE_RATE_PCT",
                size="TOTAL_COMPLAINTS",
                color="COMPANY_NAME_CLEAN",
                hover_name="COMPANY_NAME_CLEAN",
                hover_data={"TOTAL_COMPLAINTS": True, "AVG_DAYS_TO_COMPANY": True},
                title="Relief Rate % vs Consumer Dispute %",
                labels={"RELIEF_RATE_PCT": "Monetary Relief %", "DISPUTE_RATE_PCT": "Consumer Dispute %"},
            )
            fig_scatter.update_layout(height=500)
            st.plotly_chart(fig_scatter, use_container_width=True)

    # --------------------------------------------------------------------------
    # TAB 3: Product Trends Over Time
    # --------------------------------------------------------------------------
    with tab_trends:
        st.subheader("Monthly Complaint Volume by Canonical Product Category")
        trends_query = f"""
            SELECT 
                month,
                product_clean,
                SUM(complaints) AS total_complaints
            FROM {db_prefix}MART_PRODUCT_ISSUE_TRENDS
            GROUP BY month, product_clean
            ORDER BY month ASC
        """
        df_trends = run_query(conn, trends_query, engine)

        fig_line = px.line(
            df_trends,
            x="MONTH",
            y="TOTAL_COMPLAINTS",
            color="PRODUCT_CLEAN",
            title="Monthly Volume Trajectory across Products",
            labels={"MONTH": "Date", "TOTAL_COMPLAINTS": "Complaints", "PRODUCT_CLEAN": "Product"},
        )
        fig_line.update_layout(height=480, hovermode="x unified")
        st.plotly_chart(fig_line, use_container_width=True)

    # --------------------------------------------------------------------------
    # TAB 4: Data Quality & Observability Scorecard
    # --------------------------------------------------------------------------
    with tab_dq:
        st.subheader("Automated Data Quality Observability Scorecard")
        st.caption("Tracking upstream schema and data quality drift directly from Spark DQ flags.")

        dq_query = f"""
            SELECT 
                month,
                ROUND(pct_state_missing * 100, 2) AS pct_state_missing,
                ROUND(pct_state_invalid * 100, 2) AS pct_state_invalid,
                ROUND(pct_zip_masked * 100, 2) AS pct_zip_masked,
                ROUND(pct_sent_before_received * 100, 2) AS pct_sent_before_received,
                ROUND(pct_with_narrative * 100, 2) AS pct_with_narrative
            FROM {db_prefix}MART_DATA_QUALITY_SCORECARD
            ORDER BY month ASC
        """
        df_dq = run_query(conn, dq_query, engine)

        col_q1, col_q2 = st.columns(2)
        with col_q1:
            fig_dq1 = px.line(
                df_dq,
                x="MONTH",
                y=["PCT_STATE_MISSING", "PCT_STATE_INVALID", "PCT_SENT_BEFORE_RECEIVED"],
                title="Ingestion Data Anomalies Over Time (%)",
                labels={"value": "Anomaly Rate (%)", "MONTH": "Month", "variable": "Anomaly Type"},
            )
            fig_dq1.update_layout(height=400)
            st.plotly_chart(fig_dq1, use_container_width=True)

        with col_q2:
            fig_dq2 = px.line(
                df_dq,
                x="MONTH",
                y=["PCT_ZIP_MASKED", "PCT_WITH_NARRATIVE"],
                title="PII Masking & Privacy Consent Rates Over Time (%)",
                labels={"value": "Rate (%)", "MONTH": "Month", "variable": "Compliance Field"},
            )
            fig_dq2.update_layout(height=400)
            st.plotly_chart(fig_dq2, use_container_width=True)

        st.dataframe(df_dq, use_container_width=True)


if __name__ == "__main__":
    main()
