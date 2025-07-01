import streamlit as st
import pandas as pd
import requests
import logging
from app_utils import run_analysis_pipeline, _call_taktile_api

logger = logging.getLogger(__name__)

st.set_page_config(layout="wide", page_title="Automated API Run")

st.title("Track B: Automated API Ingestion (Mock)")
st.write("This simulates an automated workflow where data is pulled from a source API.")

# Note: Ensure the mock API server is running.
# You can run it with: `make start`
API_URL = "http://api:5000/api/statements" # Use the docker service name
customer_email = st.text_input("Enter customer email to fetch from API:", value="joelschaubel@gmail.com")

if st.button("Ingest from Mock API and Run Analysis"):
    if customer_email:
        logger.info(f"Starting API analysis for email: {customer_email}")
        try:
            logger.info(f"Fetching data from mock API at {API_URL} for email: {customer_email}")
            response = requests.get(API_URL, params={"email": customer_email})
            response.raise_for_status()  # Raises an exception for bad status codes

            source_data = response.json()
            if not source_data:
                st.warning(f"No data returned from API for {customer_email}.")
                logger.warning(f"No data returned from API for {customer_email}.")
            else:
                logger.info(f"Successfully fetched {len(source_data)} records from API.")
                combined_df = pd.DataFrame(source_data)
                run_analysis_pipeline(combined_df)

        except requests.exceptions.RequestException as e:
            st.error("Failed to connect to the mock API. Is it running?")
            logger.error(f"Failed to connect to mock API at {API_URL}.", exc_info=True)
        except Exception as e:
            st.error("An unexpected error occurred during API ingestion.")
            logger.error(f"An unexpected error occurred during API ingestion.", exc_info=True)
    else:
        st.warning("Please enter a customer email.")
        logger.warning("API analysis button clicked but no email was provided.")

# --- Persisted Metrics Review & Taktile Dispatch ---
if "credit_metrics_df" in st.session_state:
    credit_metrics_df = st.session_state["credit_metrics_df"]
    st.markdown("---")
    st.subheader("Review Metrics and Send to Taktile")

    if credit_metrics_df.empty:
        st.info("Credit metrics table is empty; nothing to send.")
    else:
        st.dataframe(credit_metrics_df)

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
                                f"${data.get('debt_used', 0):,.2f}",
                                f"${data.get('debt_remaining_capacity', 0):,.2f}",
                                f"${data.get('debt_guardrail', 0):,.2f}",
                                f"${data.get('credit_limit', 0):,.2f}"
                            ]
                        }
                        
                        import pandas as pd
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
