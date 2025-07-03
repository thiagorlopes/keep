# Underwriting Analysis System

## Project Goal
This project fully replicates a [Google Sheet-based underwriting process](https://docs.google.com/spreadsheets/d/18awE6NT6wYy191_cnBDhWObZPEaH3adetrLdf948VCI/edit?gid=0#gid=0) with a robust, code-driven system. It ingests raw bank statement CSVs, programmatically re-implements all the required financial metrics, and outputs a clean, auditable table containing all the calculated variables and the final `Credit Limit`.

The primary goal is to provide a system that an analyst can use to replace the manual spreadsheet, allowing them to drop in fresh CSVs and receive an accurate underwriting analysis with a single command.

## List of Resources
*   **Analyst UI**: [http://localhost:8501](http://localhost:8501)
*   **dbt Docs (Data Dictionary & Lineage)**: [http://localhost:8081](http://localhost:8081)
*   **JupyterLab (Ad-Hoc Analysis)**: [http://localhost:8888](http://localhost:8888)
*   **Mock API Interface**: [http://localhost:5000](http://localhost:5000)

## Project Structure
The project is organized into the following main directories:
- `app/`: Contains the Streamlit front-end application for the Analyst UI.
- `api_mock/`: A Flask application that mocks the Flinks API for fetching bank statements.
- `pipelines/`: Python scripts for the data ingestion and transformation pipelines (Bronze/Silver layers).
- `analytics/`: The dbt project where all the business logic and financial metrics are defined and calculated.
- `data_lake/`: A local Delta Lake store for the raw and transformed data.
- `docs/`: Project documentation.

## How to Run the System

**Prerequisites:**
*   Docker
*   `make`

### Environment Variables
Create a `.env` file in the root of the project directory with the following content:

```
# A secret key used by Flask to sign session cookies.
# You can generate a new one with: openssl rand -hex 32
API_MOCK_SECRET_KEY=a_very_secret_key_that_should_be_changed

# Your API key for the Taktile demo service.
TAKTILE_DEMO_API_KEY=your_taktile_api_key_here

# The base URL for the Taktile API.
TAKTILE_BASE_URL=https://eu-central-1.taktile-demo.decide.taktile.com
```

### Startup
From the root of the project directory, run:
```bash
make start
```
This command starts all services. To stop the application, run:
```bash
make stop
```
## Analyst Workflows

### Step 1: Running the Analysis Pipeline
The pipeline can be run from the main **[Analyst UI](http://localhost:8501)**.

#### Option A: Manual CSV Upload
1.  **Get Statement Files:** Download sample statements from the **[Mock API Interface](http://localhost:5000)**.
2.  **Upload and Run:** On the Analyst UI, upload the CSV files and click **"Run Analysis"**.

#### Option B: Automated API Run
1.  Navigate to the **[Automated API Run](http://localhost:8501/Automated_API_Run)** page.
2.  Enter a customer's email and click **"Ingest from Mock API and Run Analysis"**.

### Step 2: Exploring the Results
After a pipeline run, the final table of credit metrics (`fct_credit_metrics_by_customer`) is displayed.

For deeper, ad-hoc analysis, you can query the data directly using the **[JupyterLab environment](http://localhost:8888)**. The notebook at `analytics/analytics_development.ipynb` provides example queries.

## Taktile Integration
The final step of the analysis is to send the metrics calculated by dbt to the Taktile decisioning engine.

The Analyst UI is configured to send the `fct_credit_metrics_by_customer` table to the Taktile API. Taktile then executes the complete underwriting logic and returns the final credit limit and a detailed breakdown of the risk assessment, which is then displayed in the UI.

This architecture correctly replicates the Google Sheet logic in a robust, code-driven system, where dbt handles data transformation and Taktile handles the underwriting rules.

## Explanation of How Formulas Were Mapped
This section explains how the underwriting logic from the original [Google Sheet](https://docs.google.com/spreadsheets/d/18awE6NT6wYy191_cnBDhWObZPEaH3adetrLdf948VCI/edit?gid=0#gid=0) was mapped into the code-driven system, as required by the case study.

### Architectural Strategy: Separating Data from Logic
The core architectural decision was to separate the data preparation from the business logic. This is a modern best practice that makes the system more robust, scalable, and easier to maintain.

1.  **dbt for Data Preparation**: The dbt project is responsible for ingesting the raw transaction data and transforming it into a set of clean, aggregated financial metrics. It answers the question, "What are the key financial facts about this customer?"
2.  **Taktile for Business Logic**: The Taktile platform takes the financial facts from dbt and applies the complex, rules-based underwriting logic from the Google Sheet. It answers the question, "Given these facts, what is the customer's risk profile and credit limit?"

This approach allows the underwriting rules in Taktile to be updated by a risk analyst without needing to change any of the underlying data pipelines in dbt.

### Mapping dbt: From Raw Data to Financial Features
The formulas in the first section of the Google Sheet ("Revenue", "Debits", "Averages and balances") were mapped to a series of dbt models that progressively refine the data.

1.  **Staging and Enrichment (`stg_transactions`, `int_transactions_enriched`)**: The process begins by taking the raw transaction data and cleaning it. We standardize data types, handle missing values, and add key flags (e.g., `is_revenue`, `is_debit`) and date markers (e.g., `most_recent_statement_date_minus_90_days`).

2.  **Daily Time Series (`fct_daily_transactions_by_customer`)**: To enable accurate calculations of daily and weekly averages, we create a complete time series for each customer, ensuring there is one row for every single day. This model is critical for calculating metrics like `revised_average_balance` and `weekly_revenue`.

3.  **Final Feature Aggregation (`fct_credit_metrics_by_customer`)**: This is the final and most important dbt model. It aggregates all the daily data into a single row per customer, calculating all the foundational metrics seen in the Google Sheet. This includes:
    *   `revenue_total`
    *   `revenue_recent_90_days`
    *   `average_daily_balance_across_bank_accounts`
    *   `most_recent_balance_across_bank_accounts`
    *   `smart_revenue`
    *   `average_weekly_revenue`

The output of this dbt model is a clean table that serves as the direct input payload for the Taktile decisioning engine.

### Mapping Taktile: From Features to Credit Decision
The more complex, multi-step calculations from the "Risk Tier Estimation" and "Line Assignment" sections of the Google Sheet were mapped to a Taktile decision flow.

1.  **Risk Factor Calculation**: Taktile first takes the dbt metrics and calculates several derived risk factors, such as Change in estimated bank revenue Q Over Q `change_in_estimated_bank_revenue_qoq`).

<img width="1001" alt="image" src="https://github.com/user-attachments/assets/db197ca4-5bff-4569-880a-d6a8a2f45c83" />

2.  **Risk Scoring & Tiering**: Each risk factor is then assigned a score (typically 1, 2, or 3) based on a series of thresholds, exactly mirroring the bucketing logic in the spreadsheet. These individual scores are then averaged to produce a final `average_risk_score`. This score is then used to assign a customer to a `Risk Tier` (e.g., "Medium (Tier 3)").

<img width="1001" alt="image" src="https://github.com/user-attachments/assets/2424aa0a-1dcd-46cb-906f-35722ae3c1d9" />

3.  **Credit Line Assignment**: The assigned `Risk Tier` is used to look up key parameters for the credit line calculation, such as the `Debt Maximum Capacity Percentage` and a `Debt Guardrail` amount.

<img width="1004" alt="image" src="https://github.com/user-attachments/assets/df9bf0ca-0871-4d3e-8468-675028f56600" />

4.  **Final Approval Calculation**: Finally, the system uses these risk-adjusted parameters to calculate the `credit_limit`. It then performs the final `ROUNDDOWN` operations, as seen in the Google Sheet's "Approval Amount" section, to determine the final `credit_approval_amount` and `card_approval_amount`.

<img width="1287" alt="image" src="https://github.com/user-attachments/assets/a1690a6e-cc6f-49f6-89ea-00526e46c0b3" />

This mapping strategy successfully replicates the entire Google Sheet logic in a robust and maintainable code-driven system, fulfilling the core requirements of the case study.
