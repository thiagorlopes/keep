import streamlit as st
import pandas as pd
from deltalake import write_deltalake
import subprocess
import os
import logging
import duckdb

logger = logging.getLogger(__name__)

# Define paths
DATA_LAKE_ROOT = os.getenv("DATA_LAKE_ROOT", ".")
BRONZE_TABLE_PATH = os.path.join(DATA_LAKE_ROOT, "data_lake/bronze/statements")
DBT_DB_PATH = os.path.join(DATA_LAKE_ROOT, "data_lake/dbt.duckdb")
ANALYTICS_TABLE_NAME = "fct_daily_transactions_by_customer"


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
                ["dbt", "run"],
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
            logger.info(f"{log_prefix} Successfully loaded {len(final_df)} rows from analytics table.")
            logger.info(f"{log_prefix} Final DataFrame shape: {final_df.shape}")
            logger.info(f"{log_prefix} Final DataFrame columns: {final_df.columns.to_list()}")
            logger.info(f"{log_prefix} Final DataFrame schema:\n{final_df.dtypes.to_string()}")

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
        
        # --- Taktile Simulation ---
        st.markdown("---")
        st.subheader("Hypothetical: Send to Decisioning Engine")
        st.write("This section simulates generating a payload for a decisioning engine like Taktile.")

        if st.button("Generate Taktile API Payload (Mock)"):
            logger.info(f"{log_prefix} Generating mock Taktile payload...")
            payload = {
                "customer_email": final_df['email'].iloc[0],
                "request_id": final_df['request_id'].iloc[0],
                "analysis_timestamp": pd.Timestamp.now().isoformat(),
                "key_metrics": {
                    "average_revised_balance": final_df['revised_average_balance'].mean(),
                    "minimum_revised_balance": final_df['revised_average_balance'].min(),
                    "maximum_revised_balance": final_df['revised_average_balance'].max(),
                    "days_of_history": len(final_df['date'].unique()),
                },
                "daily_balances": final_df[['date', 'revised_average_balance']].to_dict('records') # type: ignore
            }
            st.json(payload)
            st.success("Payload generated. In a real application, this would be sent via an API POST request.")
            logger.info(f"{log_prefix} Mock Taktile payload generated and displayed.")

    except subprocess.CalledProcessError as e:
        st.error("An error occurred during a data processing step. Please check the application logs or contact support for assistance.")
        logger.error(f"{log_prefix} A subprocess failed. Return code: {e.returncode}")
        logger.error(f"{log_prefix} stdout: {e.stdout}")
        logger.error(f"{log_prefix} stderr: {e.stderr}")
    except Exception as e:
        st.error("An unexpected application error occurred. Please contact support.")
        logger.error(f"{log_prefix} An unexpected error occurred: {str(e)}", exc_info=True) 
