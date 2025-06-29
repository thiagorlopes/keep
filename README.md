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

Once the system is running, you can use the following tools to perform your analysis.

### Primary Workflow: The Analyst UI
**Link:** [http://localhost:8501](http://localhost:8501)

This is the main web application for day-to-day underwriting tasks. It provides two methods for analyzing customer statements.

#### 1. Manual CSV Upload
This workflow is for analyzing statements that you have saved on your computer.

1.  On the main page, drag and drop one or more customer statement CSVs into the uploader.
2.  Click the **"Run Analysis on Uploaded CSVs"** button.
3.  The system will process the files and display the results, including key metrics and visualizations.

#### 2. Automated API Run
This workflow is for analyzing a customer's data by pulling it directly from the system.

1.  Using the sidebar, navigate to the **"Automated API Run"** page.
2.  Enter the customer's email address and click **"Ingest from Mock API and Run Analysis"**.
3.  The system will fetch the data, process it, and display the results.

#### Final Review and Approval
For both workflows, after the analysis is displayed, you have a final review step. If you are comfortable with the results, you can click the **"Generate Taktile API Payload"** button. This simulates sending the final, approved features to the decisioning engine.

### Interactive Analysis with Jupyter
**Link:** [http://localhost:8888](http://localhost:8888)

For analysts who want to dig deeper, perform ad-hoc queries, or validate intermediate data, a JupyterLab environment is provided.

1.  Navigate to the JupyterLab link above.
2.  In the file browser on the left, you can see the project's directory structure, including the `/data_lake` and `/analytics` folders.
3.  You can open the `analytics/analytics_development.ipynb` notebook as a starting point, or create a new one to directly query the data using DuckDB (e.g., `SELECT * FROM read_parquet('data_lake/silver/*.parquet')`).

### Other Resources
*   **Live Data Model Documentation:** [http://localhost:8081](http://localhost:8081) - An interactive website showing how all the data models are calculated and related.
*   **Mock API Interface:** [http://localhost:5000](http://localhost:5000) - A simple UI to explore the raw data available from the mock API.
