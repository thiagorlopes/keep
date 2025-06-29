import streamlit as st
import pandas as pd
import sys
import os
import logging
from app_utils import run_analysis_pipeline

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
