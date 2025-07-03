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
- `docs/`: Complementary project documentation.

## 1. Environment Setup

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
## 2. How to Run the System
### Step 1: Running the Analysis Pipeline
There are two ways to run the pipeline, both available in the main **Analyst UI**:

**Link:** [http://localhost:8501](http://localhost:8501)

<img width="800" alt="image" src="https://github.com/user-attachments/assets/423fbfb3-1d92-4efb-bdb7-e06cd3703808" />

#### Option A: Manual CSV Upload
This workflow is for analyzing statements that you have saved on your computer.

1.  **Get Statement Files:**

First, you need statement files to analyze. You can get sample files from the included Mock API:
<img width="800" alt="image" src="https://github.com/user-attachments/assets/f1989932-d172-4ef4-ae08-32bcfbec1a7c" />
*   Navigate to the **Mock API Interface** at [http://localhost:5000](http://localhost:5000).
*   Enter a customer email (e.g., `joelschaubel@gmail.com`) and click "Download Statements".
*   A `.zip` file containing CSV statements will be downloaded. Unzip this file.

2.  **Upload and Run:**
*   On the main page of the [Analyst UI](http://localhost:8501), drag and drop the downloaded CSV files into the uploader.
*   Click the **"Run Analysis on Uploaded CSVs"** button.
3.  **View Results:** The system will process the files and display a summary of the results.

#### Option B: Automated API Run
This workflow is for analyzing a customer's data by pulling it directly from the mocked Flinks system.
1.  Using the sidebar, navigate to the [Automated API Run](http://localhost:8501/Automated_API_Run) page.
2.  Enter the customer's email address and click **"Ingest from Mock API and Run Analysis"**.
3.  The system will fetch the data, process it, and display a summary.

### Step 2: Exploring the Results
After a pipeline run is complete, you can dig deeper into the data and the business logic using the following tools.

#### Understanding the Data: dbt Docs
First, to understand what tables were created, what each column means, and to see the business logic, use the live data documentation.
*   **Link:** [http://localhost:8081](http://localhost:8081)

The dbt docs provide an interactive dependency graph, so you can check the relationship between the models:

![image](https://github.com/user-attachments/assets/a21e5a9a-9f54-4c86-9354-d46de09fc657)

It also provides a [full data dictionary](http://localhost:8081/#!/model/model.customer_transactions.all_transactions_by_customer), making it easy to understand how the metrics are calculated.

<img width="800" alt="image" src="https://github.com/user-attachments/assets/dd6e0cc0-4bbe-4b70-b137-5c522a2f7cb1" />

#### Querying the Data: JupyterLab
For hands-on, ad-hoc analysis, you can use the provided JupyterLab environment to write your own SQL queries against the generated data.
*   **Link:** [http://localhost:8888](http://localhost:8888)

<img width="1250" alt="image" src="https://github.com/user-attachments/assets/8868ac45-b3d6-4393-acff-bf0e1a26a393" />

**Example:** To query the daily transactions table, you can access the notebook [analytics/analytics_development.ipynb](http://localhost:8888/lab/tree/analytics/analytics_development.ipynb). Next you can visualize by inputting the string "fct_daily_transactions_by_customer" in the input box and clicking on `Load Model`. Also, you can query data from the dbt model in the following manner:

```sql
SELECT * FROM main.fct_daily_transactions_by_customer
```

Remember that, in dbt, you reference an input model as {{ ref('model_name') }}, while, in the notebook, you should use main.model_name.

### Step 3: Making a Decision with Taktile
The final step of the workflow is to take the calculated credit metrics and use them to make an underwriting decision. This project uses [Taktile](https://app.taktile.com/decide/org/976b336b-e6b4-4904-8e20-1e27c33dc099/ws/3bba1a21-aaa5-457b-af1b-422404e0e960/flows?folder-id=8f6e925b-ea9e-46e4-a30a-953c4c418d9d), a modern decisioning platform, to model the decision flow.

After a pipeline run is complete, the Analyst UI will display the final `fct_credit_metrics_by_customer` table. 

<img width="1440" alt="image" src="https://github.com/user-attachments/assets/e94ed16e-f1aa-4bac-8bd2-b6a4dcd7b762" />

The next steps then are:

1.  Review the calculated metrics in the UI.
2.  Click the **"Send to Taktile"** button.
3.  The application will send the metrics to the Taktile API and display the full decision response, including the final approval amount and risk analysis.

The picture below shows part of the decision flow in Taktile.

![image](https://github.com/user-attachments/assets/fe9e8f77-1a32-4e93-95f6-6cbd08a45a5b)

After the automated business decision in Taktile, the results will be rendered in the Analyst UI:

<img width="1423" alt="image" src="https://github.com/user-attachments/assets/5642fbf3-3e1d-4de0-98d0-63bb549ee8ce" />

This final information can be used internally at Keep for approving the limit for the customer.

## 3. Explanation of How Formulas Were Mapped
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

For a complete, column-level data dictionary, technical implementation details, and an interactive data lineage graph for all dbt models, please refer to the live **[dbt Docs site](http://localhost:8081)**.

<img width="1200" alt="image" src="https://github.com/user-attachments/assets/826d3fa4-eb14-43a6-96c9-4ebbfe86401a" />

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
