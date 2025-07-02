import streamlit as st
import pandas as pd
import requests
import logging
from app_utils import run_analysis_pipeline, display_taktile_interface

logger = logging.getLogger(__name__)

st.set_page_config(layout="wide", page_title="Automated API Run")

st.title("Automated API Ingestion")
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
display_taktile_interface(st, logger)
