import streamlit as st
import pandas as pd
from deltalake import write_deltalake
import subprocess
import os
import logging
import duckdb
import requests
import time
import numpy as np
import json

logger = logging.getLogger(__name__)

# Define paths
DATA_LAKE_ROOT = os.getenv("DATA_LAKE_ROOT", ".")
BRONZE_TABLE_PATH = os.path.join(DATA_LAKE_ROOT, "data_lake/bronze/statements")
DBT_DB_PATH = os.path.join(DATA_LAKE_ROOT, "data_lake/dbt.duckdb")
ANALYTICS_TABLE_NAME = "fct_daily_transactions_by_customer"

# Taktile configuration
TAKTILE_API_KEY = os.getenv("TAKTILE_DEMO_API_KEY")
TAKTILE_BASE_URL = os.getenv("TAKTILE_BASE_URL", "https://eu-central-1.taktile-demo.decide.taktile.com")

def _convert_to_json_serializable(obj):
    """Convert numpy/pandas data types to JSON-serializable Python types."""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {k: _convert_to_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_convert_to_json_serializable(item) for item in obj]
    elif pd.isna(obj):
        return None
    else:
        return obj

def run_analysis_pipeline(source_df: pd.DataFrame):
    """
    Runs the full analysis pipeline from a given source DataFrame.
    """
    if source_df.empty:
        st.warning("The source data is empty. Please provide valid data.")
        logger.warning("run_analysis_pipeline called with an empty DataFrame.")
        return

    # Extract a unique identifier for logging and traceability.
    # We'll use the email as it's a reliable unique identifier.
    try:
        email = source_df['Email'].unique()[0]
        log_prefix = f"[{email}] -"
    except (KeyError, IndexError):
        email = "Unknown"
        log_prefix = "[Unknown Email] -"

    try:
        logger.info(f"{log_prefix} Pipeline started with source data of shape: {source_df.shape}")
        logger.info(f"{log_prefix} Source columns: {source_df.columns.to_list()}")
        with st.spinner("Running underwriting analysis... This may take a moment."):
            # --- Step 1: Processing Statements ---
            logger.info(f"{log_prefix} Starting Step 1/3: Processing bank statements.")
            logger.info(f"{log_prefix} Writing {len(source_df)} rows to Bronze layer at {BRONZE_TABLE_PATH}...")
            logger.info(f"{log_prefix} Source schema:\n{source_df.dtypes.to_string()}")
            write_deltalake(BRONZE_TABLE_PATH, source_df, mode="overwrite", schema_mode="overwrite")
            logger.info(f"{log_prefix} Successfully wrote to Bronze layer.")

            logger.info(f"{log_prefix} Kicking off transformation pipeline subprocess with overwrite mode...")
            transform_process = subprocess.run(
                ["python", "-m", "pipelines.transform_statements", "--write-mode", "overwrite"],
                check=True,
                capture_output=True,
                text=True
            )
            logger.info(f"{log_prefix} Transformation pipeline stdout:\n{transform_process.stdout}")
            logger.info(f"{log_prefix} Transformation pipeline subprocess completed successfully.")

            # --- Step 2: Calculating Underwriting Metrics ---
            logger.info(f"{log_prefix} Starting Step 2/3: Calculating underwriting metrics via dbt.")
            dbt_process = subprocess.run(
                ["dbt", "run", "--full-refresh"],
                cwd="analytics",
                check=True,
                capture_output=True,
                text=True
            )
            logger.info(f"{log_prefix} dbt models subprocess completed successfully.")
            if dbt_process.stdout:
                logger.info(f"{log_prefix} dbt stdout:\n{dbt_process.stdout}")

            logger.info(f"{log_prefix} Generating dbt documentation...")
            dbt_docs_process = subprocess.run(
                ["dbt", "docs", "generate"],
                cwd="analytics",
                check=True,
                capture_output=True,
                text=True
            )
            logger.info(f"{log_prefix} dbt docs generation completed successfully.")

            # --- Step 3: Generating Final Report ---
            logger.info(f"{log_prefix} Starting Step 3/3: Reading final results from dbt database at {DBT_DB_PATH}...")
            with duckdb.connect(DBT_DB_PATH, read_only=True) as con:
                final_df = con.table(ANALYTICS_TABLE_NAME).to_df()
                try:
                    credit_metrics_df = con.table("fct_credit_metrics_by_customer").to_df()
                except Exception as e:
                    logger.warning(f"{log_prefix} Could not load credit metrics table: {e}")
                    credit_metrics_df = pd.DataFrame()
            logger.info(f"{log_prefix} Successfully loaded {len(final_df)} rows from analytics table.")
            logger.info(f"{log_prefix} Final DataFrame shape: {final_df.shape}")
            logger.info(f"{log_prefix} Final DataFrame columns: {final_df.columns.to_list()}")
            logger.info(f"{log_prefix} Final DataFrame schema:\n{final_df.dtypes.to_string()}")

            if not credit_metrics_df.empty:
                logger.info(f"{log_prefix} Loaded credit metrics DataFrame with shape {credit_metrics_df.shape}")

        # --- If successful, show results ---
        st.success("Analysis Complete!")
        logger.info(f"{log_prefix} Analysis Complete!")

        st.subheader("Underwriting Analysis Results")
        st.write("The table below shows the daily revised average balance for the customer over the last 180 days.")

        if final_df.empty:
            st.warning("The analysis completed, but there are no results to display.")
            return

        for request_id, group in final_df.groupby('request_id'):
            email = group['email'].iloc[0]
            st.markdown(f"#### Customer: `{email}`")
            st.dataframe(group[['date', 'revised_average_balance']].style.format({"revised_average_balance": "${:,.2f}"}))
            st.line_chart(group.rename(columns={'date':'index'}).set_index('index')['revised_average_balance'])

        st.balloons()

        # Store results in session_state so they persist across reruns
        st.session_state["credit_metrics_df"] = credit_metrics_df
        st.session_state["final_df"] = final_df

    except subprocess.CalledProcessError as e:
        st.error("An error occurred during a data processing step. Please check the application logs or contact support for assistance.")
        logger.error(f"{log_prefix} A subprocess failed. Return code: {e.returncode}")
        logger.error(f"{log_prefix} stdout: {e.stdout}")
        logger.error(f"{log_prefix} stderr: {e.stderr}")
    except Exception as e:
        st.error("An unexpected application error occurred. Please contact support.")
        logger.error(f"{log_prefix} An unexpected error occurred: {str(e)}", exc_info=True)

def _test_taktile_connection(logger: logging.Logger) -> dict:
    """Test Taktile API connection by getting flow versions."""
    if not TAKTILE_API_KEY:
        raise EnvironmentError("TAKTILE_DEMO_API_KEY environment variable is not set.")

    # First, test basic connectivity to the base URL
    logger.info(f"Testing basic connectivity to: {TAKTILE_BASE_URL}")
    try:
        basic_response = requests.get(TAKTILE_BASE_URL, timeout=10)
        logger.info(f"Base URL response status: {basic_response.status_code}")
    except Exception as e:
        logger.error(f"Cannot reach base URL: {e}")
        return {"error": f"Cannot reach base URL: {e}"}

    # Try different authentication formats
    auth_formats = [
        {"X-Api-Key": TAKTILE_API_KEY},  # This is the correct format from Taktile docs
        {"Authorization": f"Api-Key {TAKTILE_API_KEY}"},
        {"Authorization": f"Bearer {TAKTILE_API_KEY}"},
        {"Authorization": f"Token {TAKTILE_API_KEY}"},
        {"X-API-Key": TAKTILE_API_KEY},
    ]

    # Try to get flow versions to test connection
    versions_url = f"{TAKTILE_BASE_URL}/run/api/v1/flows/underwriting/versions"
    logger.info(f"Testing Taktile connection: {versions_url}")

    for i, auth_header in enumerate(auth_formats):
        headers = {
            **auth_header,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        try:
            logger.info(f"Trying auth format {i+1}: {auth_header}")
            response = requests.get(versions_url, headers=headers, timeout=10)
            logger.info(f"Auth format {i+1} response status: {response.status_code}")

            if response.status_code == 200:
                logger.info(f"✅ Success with auth format {i+1}: {auth_header}")
                return {"success": True, "auth_format": auth_header, "data": response.json()}
            elif response.status_code != 401:
                # If we get a different error (not auth), that might be progress
                logger.info(f"Non-401 response with format {i+1}: {response.status_code}")
                return {"partial_success": True, "auth_format": auth_header, "status": response.status_code, "response": response.text}
            else:
                logger.info(f"❌ 401 with auth format {i+1}")

        except Exception as e:
            logger.error(f"Exception with auth format {i+1}: {e}")

    # If all auth formats fail, try without auth to see what we get
    logger.info("Trying without authentication to see the error...")
    try:
        no_auth_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        response = requests.get(versions_url, headers=no_auth_headers, timeout=10)
        logger.info(f"No auth response: {response.status_code} - {response.text}")
        return {"error": "All authentication formats failed", "no_auth_response": response.text, "status": response.status_code}
    except Exception as e:
        logger.error(f"No auth test failed: {e}")

    return {"error": "All authentication formats failed with 401"}

def _call_taktile_api(payload: dict, logger: logging.Logger) -> dict:
    """Helper to call Taktile underwriting flow and return the final decision payload.

    This helper handles both synchronous (200) and asynchronous (202) responses.
    """
    if not TAKTILE_API_KEY:
        raise EnvironmentError("TAKTILE_DEMO_API_KEY environment variable is not set.")

    # Convert payload to JSON-serializable format
    serializable_payload = _convert_to_json_serializable(payload)

    headers = {
        "X-Api-Key": TAKTILE_API_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    decide_url = f"{TAKTILE_BASE_URL}/run/api/v1/flows/underwriting/sandbox/decide"
    logger.info(f"Posting payload to Taktile: {decide_url}")
    logger.info(f"Using API key: {TAKTILE_API_KEY[:10]}...{TAKTILE_API_KEY[-4:] if TAKTILE_API_KEY and len(TAKTILE_API_KEY) > 14 else 'INVALID'}")
    logger.info(f"Request headers: {headers}")
    response = requests.post(decide_url, headers=headers, json=serializable_payload, timeout=30)

    # Log the response details for debugging
    logger.info(f"Response status: {response.status_code}")
    logger.info(f"Response headers: {dict(response.headers)}")
    if response.status_code != 200:
        logger.error(f"Error response body: {response.text}")

    response.raise_for_status()

    resp_json = response.json()

    # If Taktile returns 202 we need to poll for the result
    if response.status_code == 202:
        decision_id = resp_json["metadata"]["decision_id"]
        status_url = f"{TAKTILE_BASE_URL}/run/api/v1/flows/underwriting/sandbox/decide/{decision_id}"
        logger.info(f"Decision accepted (202). Polling for result using {status_url} ...")
        for _ in range(15):  # poll ~30 s max (15×2 s)
            time.sleep(2)
            poll_resp = requests.get(status_url, headers=headers, timeout=15)
            if poll_resp.status_code == 200:
                return poll_resp.json()
        # If polling loop finishes without success, raise
        poll_resp.raise_for_status()
    return resp_json

def display_taktile_interface(st, logger):
    """
    Renders the Taktile interaction interface in Streamlit,
    including the button to send the request and the display of the response.
    """
    if "credit_metrics_df" in st.session_state:
        credit_metrics_df = st.session_state["credit_metrics_df"]
        st.markdown("---")
        st.subheader("Review Metrics and Send to Taktile")

        if credit_metrics_df.empty:
            st.info("Credit metrics table is empty; nothing to send.")
        else:
            st.dataframe(credit_metrics_df)

            if st.button("Send to Taktile"):
                with st.spinner("Sending to Taktile..."):
                    try:
                        row = credit_metrics_df.iloc[0]
                        
                        data_payload = {
                            "request_id": row.get("request_id", 0),
                            "email": row.get("email", 0),
                            "revenue_total_credit": row.get("revenue_total_credit", 0),
                            "revenue_total": row.get("revenue_total", 0),
                            "revenue_recent_90_days": row.get("revenue_recent_90_days", 0),
                            "revenue_91_to_180_days": row.get("revenue_91_to_180_days", 0),
                            "debits_total": row.get("debits_total", 0),
                            "debits_recent_90_days": row.get("debits_recent_90_days", 0),
                            "debits_91_to_180_days": row.get("debits_91_to_180_days", 0),
                            "credit_card_payments": row.get("credit_card_payments", 0),
                            "credit_card_recent_90_days": row.get("credit_card_recent_90_days", 0),
                            "credit_card_91_to_180_days": row.get("credit_card_91_to_180_days", 0),
                            "average_daily_balance_across_bank_accounts": row.get("average_daily_balance_across_bank_accounts", 0),
                            "most_recent_balance_across_bank_accounts": row.get("most_recent_balance_across_bank_accounts", 0),
                            # Sourced directly from dbt model
                            "estimated_annual_revenue": row.get("estimated_annual_revenue", 0),
                            "average_daily_balance": row.get("average_daily_balance", 0),
                            "average_daily_revenue": row.get("average_daily_revenue", 0),
                            "smart_revenue": row.get("smart_revenue", 0),
                            "existing_debt_payments_consideration": row.get("existing_debt_payments_consideration", 0),
                            "average_weekly_revenue": row.get("average_weekly_revenue", 0),
                            "years_in_business": "More than 5 years", # This remains as it's not in the model
                        }
                        takt_payload = {
                            "data": data_payload,
                            "metadata": {"entity_id": str(row["request_id"])},
                            "control": {"execution_mode": "sync"},
                        }
                        decision_resp = _call_taktile_api(takt_payload, logger)
                        st.success("Decision received from Taktile!")
                        
                        # Display key decision metrics in a user-friendly format
                        if "data" in decision_resp:
                            data = decision_resp["data"]
                            
                            # Risk Tier Estimation Section (moved to first)
                            st.subheader("Risk Tier Estimation")
                            risk_estimation_data = {
                                "Metric": [
                                    "Average Risk Score",
                                    "Average daily Balance to Estimated Monthly Revenue",
                                    "Debt/Revenue Ratio",
                                    "Change in estimated Bank Revenue Q over Q (Bank)",
                                    "Current Balance to Avg Balance in last 6 months",
                                    "Years in Business",
                                    "Monthly burn",
                                    "Runway (Months)"
                                ],
                                "Values": [
                                    str(data.get('average_risk_score', 'N/A')),
                                    f"{data.get('average_daily_balance_to_estimated_monthly_revenue', 0):.2%}",
                                    f"{data.get('debt_revenue_ratio', 0):.2%}",
                                    f"{data.get('change_in_estimated_bank_revenue_qoq', 0):.2%}",
                                    f"{data.get('current_balance_to_avg_balance_in_last_6_months', 0):.2%}",
                                    "More than 5 years",  # Use the value we sent to Taktile
                                    f"${data.get('monthly_burn', 0):,.2f}",
                                    f"{data.get('runway_months', 0):,.2f}" if data.get('runway_months') is not None else "N/A"
                                ]
                            }
                            
                            risk_df = pd.DataFrame(risk_estimation_data)
                            st.dataframe(risk_df, hide_index=True, use_container_width=True)
                            
                            # Line Assignment Section
                            st.subheader("Line Assignment")
                            
                            # Create data for the table
                            line_assignment_data = {
                                "Metric": [
                                    "Maximum Debt Capacity",
                                    "Already Used", 
                                    "Remaining Capacity",
                                    "Guardrail",
                                    "Credit Limit"
                                ],
                                "Amount": [
                                    f"${data.get('debt_maximum_capacity', 0):,.2f}",
                                    f"${data.get('debt_used', 0):,.2f}",  # Use actual API data
                                    f"${data.get('debt_remaining_capacity', 0):,.2f}",
                                    f"${data.get('debt_guardrail', 0):,.2f}",
                                    f"${data.get('credit_limit', 0):,.2f}"
                                ]
                            }
                            
                            line_df = pd.DataFrame(line_assignment_data)
                            st.dataframe(line_df, hide_index=True, use_container_width=True)
                            
                            # Approval Amount (highlighted)
                            st.markdown("### Approval Amount")
                            approval_amount = data.get("credit_approval_amount", 0)
                            st.markdown(f"<div style='background-color: #f0f0f0; padding: 10px; text-align: center; font-size: 24px; font-weight: bold;'>${approval_amount:,.2f}</div>", unsafe_allow_html=True)
                            
                            # Card's MCA Section
                            st.subheader("Card's MCA")
                            card_mca_data = {
                                "Metric": [
                                    "Avg Weekly Revenue",
                                    "Avg Calculated Daily Revenue",
                                    "Daily withdrawal",
                                    "MAX approval amount",
                                    "Card Approval Amount"
                                ],
                                "Amount": [
                                    f"${data.get('average_weekly_revenue', 0):,.2f}",
                                    f"${data.get('average_calculated_daily_revenue', 0):,.2f}",
                                    f"${data.get('daily_withdrawal', 0):,.2f}",
                                    f"${data.get('card_max_approval_amount', 0):,.2f}",
                                    f"${data.get('card_approval_amount', 0):,.2f}"
                                ]
                            }
                            
                            card_df = pd.DataFrame(card_mca_data)
                            st.dataframe(card_df, hide_index=True, use_container_width=True)
                            
                            # Capital MCA Section
                            st.subheader("Capital MCA")
                            capital_mca_data = {
                                "Metric": [
                                    "Risk Tier",
                                    "Length Multiplier (in weeks)",
                                    "Repayment Frequency (daily, weekly, monthly)",
                                    "Max Approval Amount",
                                    "Capital MCA Approval Amount",
                                    "Capital+ Approval Amount"
                                ],
                                "Value": [
                                    data.get("risk_tier", "N/A"),
                                    str(data.get("length_multiplier_in_weeks", 0)),
                                    data.get("repayment_frequency", "N/A"),
                                    f"${data.get('capital_max_approval_amount', 0):,.2f}",
                                    f"${data.get('capital_mca_approval_amount', 0):,.2f}",
                                    f"${data.get('capital_approval_amount', 0):,.2f}"
                                ]
                            }
                            
                            capital_df = pd.DataFrame(capital_mca_data)
                            st.dataframe(capital_df, hide_index=True, use_container_width=True)
                            
                            # Expandable detailed response
                            with st.expander("Full Response Details"):
                                st.json(decision_resp)
                        
                        else:
                            st.warning("Unexpected response format from Taktile")
                            st.json(decision_resp)
                        
                    except Exception as e:
                        st.error(f"Taktile request failed: {e}")
