import streamlit as st
import pandas as pd
import sys
import os
import logging
from app_utils import run_analysis_pipeline, _call_taktile_api, _test_taktile_connection

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Ensure the app can find other modules, if necessary
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

st.set_page_config(layout="wide", page_title="Manual CSV Upload")

st.title("Underwriting Analysis Workflow")

st.write(
    """
    Welcome to the Underwriting Analysis tool.
    This application processes raw bank statement CSVs to generate key underwriting metrics.

    **Instructions:**
    1. Upload one or more bank statement CSV files.
    2. Click the 'Run Underwriting Analysis' button.
    3. The results, including daily balances and charts, will be displayed below.
    """
)

st.header("Track A: Manual CSV Upload")
st.write("This is the standard workflow for an analyst to manually upload and process statements.")

uploaded_files = st.file_uploader(
    "Upload Bank Statement CSVs",
    type="csv",
    accept_multiple_files=True
)

if st.button("Run Analysis on Uploaded CSVs"):
    if uploaded_files:
        logger.info(f"Starting CSV analysis for {len(uploaded_files)} uploaded files.")
        run_analysis_pipeline(pd.concat([pd.read_csv(file) for file in uploaded_files], ignore_index=True))
    else:
        st.warning("Please upload at least one CSV file.")
        logger.warning("CSV analysis button clicked but no files were uploaded.")

# --- Persisted Metrics Review & Taktile Dispatch ---
if "credit_metrics_df" in st.session_state:
    credit_metrics_df = st.session_state["credit_metrics_df"]
    st.markdown("---")
    st.subheader("Review Metrics and Send to Taktile")

    if credit_metrics_df.empty:
        st.info("Credit metrics table is empty; nothing to send.")
    else:
        st.dataframe(credit_metrics_df)

        col1, col2 = st.columns(2)

        with col1:
            if st.button("Test Taktile Connection"):
                with st.spinner("Testing Taktile API connection..."):
                    try:
                        test_result = _test_taktile_connection(logging.getLogger("taktile_test"))
                        if "error" in test_result:
                            st.error(f"Connection test failed: {test_result['error']}")
                        else:
                            st.success("✅ Connection successful!")
                            st.json(test_result)
                    except Exception as e:
                        st.error(f"Connection test failed: {e}")

        with col2:
            if st.button("Send to Taktile"):
                row = credit_metrics_df.iloc[0]

                # Calculate derived metrics to avoid division by zero
                revenue_total = float(row["revenue_total"]) if row["revenue_total"] > 0 else 1.0
                estimated_annual_revenue = revenue_total * 365 / 180 if revenue_total > 0 else 1.0  # Extrapolate from 6 months
                average_daily_revenue = revenue_total / 180 if revenue_total > 0 else 1.0
                average_weekly_revenue = average_daily_revenue * 7
                smart_revenue = estimated_annual_revenue  # Use estimated annual as smart revenue
                average_daily_balance = float(row["average_daily_balance_across_bank_accounts"]) if row["average_daily_balance_across_bank_accounts"] > 0 else 1.0

                data_payload = {
                    "request_id": row["request_id"],
                    "email": row["email"],
                    "revenue_total_credit": row["revenue_total_credit"],
                    "revenue_total": row["revenue_total"],
                    "revenue_recent_90_days": row["revenue_recent_90_days"],
                    "revenue_91_to_180_days": row["revenue_91_to_180_days"],
                    "debits_total": row["debits_total"],
                    "debits_recent_90_days": row["debits_recent_90_days"],
                    "debits_91_to_180_days": row["debits_91_to_180_days"],
                    "credit_card_payments": row["credit_card_payments"],
                    "credit_card_recent_90_days": row["credit_card_recent_90_days"],
                    "credit_card_91_to_180_days": row["credit_card_91_to_180_days"],
                    "average_daily_balance_across_bank_accounts": row["average_daily_balance_across_bank_accounts"],
                    "most_recent_balance_across_bank_accounts": row["most_recent_balance_across_bank_accounts"],
                    # Required calculated fields
                    "estimated_annual_revenue": estimated_annual_revenue,
                    "average_daily_balance": average_daily_balance,
                    "average_daily_revenue": average_daily_revenue,
                    "smart_revenue": smart_revenue,
                    "existing_debt_payments_consideration": 0,
                    "average_weekly_revenue": average_weekly_revenue,
                    "years_in_business": "More than 5 years",
                }
                takt_payload = {
                    "data": data_payload,
                    "metadata": {"version": "v1.0", "entity_id": str(row["request_id"])} ,
                    "control": {"execution_mode": "sync"},
                }
                with st.spinner("Sending to Taktile..."):
                    try:
                        decision_resp = _call_taktile_api(takt_payload, logging.getLogger("taktile_api"))
                        st.success("Decision received from Taktile!")
                        st.json(decision_resp)
                    except Exception as e:
                        st.error(f"Taktile request failed: {e}")
