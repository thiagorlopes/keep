# Underwriting Analysis System

This application provides a simple, robust, and reliable way to process bank statements and generate the necessary metrics for underwriting analysis.

It replaces the manual [Google Sheet](https://docs.google.com/spreadsheets/d/18awE6NT6wYy191_cnBDhWObZPEaH3adetrLdf948VCI/edit?gid=0#gid=0) process with a code-driven system, ensuring that calculations are consistent, auditable, and transparent.

## How to Run the System

**Prerequisites:**
*   Docker
*   `make`

From the root of the project directory, simply run:

```bash
make start
```
This single command will start all the necessary services.

## Analyst Workflows

The process for an analyst is straightforward: first, you run the main pipeline to process customer statements. Afterwards, you can use the provided tools to explore the results.

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

<img width="800" alt="image" src="https://github.com/user-attachments/assets/ccf9b7e9-69f5-404b-b384-9010076b921a" />

It also provides a [full data dictionary](http://localhost:8081/#!/model/model.customer_transactions.all_transactions_by_customer), making it easy to understand how the metrics are calculated.

<img width="800" alt="image" src="https://github.com/user-attachments/assets/ea757dc3-300e-4a2a-a27f-cf34c85a0465" />

#### Querying the Data: JupyterLab
For hands-on, ad-hoc analysis, you can use the provided JupyterLab environment to write your own SQL queries against the generated data.
*   **Link:** [http://localhost:8888](http://localhost:8888)

<img width="800" alt="image" src="https://github.com/user-attachments/assets/8256630b-b6fb-4c9d-ae83-d51ed8ff0b5c" />

**Example:** To query the daily transactions table, you can access the notebook [analytics/analytics_development.ipynb](http://localhost:8888/lab/tree/analytics/analytics_development.ipynb). You should always run the first cell, which defines the method `run_query`. After that, you can query data from the dbt model in the following manner:

```python
result_dataframe = run_query("SELECT * FROM main.fct_daily_transactions_by_customer")
result_dataframe.head()
```

Remember that, in dbt, you reference an input model as {{ ref('model_name') }}, while, in the notebook, you should use main.model_name.

### List of Resources
*   **Mock API Interface:** [http://localhost:5000](http://localhost:5000)
*   **Analyst UI**: [http://localhost:8501](http://localhost:8501)
*   **dbt Docs**: [http://localhost:8081](http://localhost:8081)
*   **JupyterLab**: [http://localhost:8888](http://localhost:8888)
