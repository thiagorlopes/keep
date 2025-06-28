# Data & Analytics Pipeline

This document provides a detailed overview of the data engineering pipelines responsible for ingesting, transforming, and managing the data for the credit scoring system.

## Architectural Philosophy

The pipelines are designed around modern data engineering best practices, emphasizing reliability, scalability, and maintainability.

- **Transactional Data Lake**: We use a **Delta Lake** architecture. Instead of relying on brittle, manual file management, all data is stored in ACID-compliant Delta tables. This prevents data corruption and provides a reliable, single source of truth.
- **Idempotency by Design**: Every pipeline run is idempotent. The use of atomic `MERGE` operations guarantees that re-running a pipeline will not create duplicate data or cause errors. This is critical for reliable financial data processing.
- **Multi-Hop Architecture**: We follow a standard Bronze/Silver multi-hop pattern. This provides clear data lineage, allowing for reprocessing and auditing at any stage of the pipeline.

## Core Components

The data lake is co-located with the pipelines at `pipelines/data_lake/` and consists of three key tables.

### 1. The Bronze Table (`data_lake/bronze`)

- **Purpose**: To serve as the single, immutable source of all raw data ingested from the upstream API.
- **Schema**: The schema is kept as close to the source as possible to maintain a true historical record. The only addition is an `account_id` column to trace each transaction back to its source account.
- **Process**: The `ingest_statements.py` script fetches data from the API and appends it to this table.

### 2. The Silver Table (`data_lake/silver`)

- **Purpose**: To provide a clean, standardized, and enriched dataset ready for analytics.
- **Schema**: This table represents our canonical view of a transaction. Column names are standardized (e.g., snake_cased), data types are enforced, and missing values are handled appropriately.
- **Process**: The `transform_statements.py` script reads from the bronze table, performs cleaning operations, and uses a `MERGE` operation to upsert the cleaned data into the silver table. The merge is based on a composite key of `(email, request_id, date, description)` to prevent duplicate transaction records.

### 3. The Application Status Ledger (`data_lake/application_status_ledger`)

- **Purpose**: This table acts as a state machine or a "work queue" for our process. It tracks the lifecycle of each credit application as it moves through the system.
- **Schema**: It contains the unique identifiers for an application (`email`, `request_id`) and its current `status` (e.g., `PENDING`, `SENT`, `SCORED`).
- **Process**: After the `transform_statements` pipeline cleans a set of transactions, it performs a `MERGE` operation on this ledger. It looks for new `(email, request_id)` pairs and inserts them with a `PENDING` status. Because it's a merge, existing applications are untouched, guaranteeing exactly-once processing for downstream systems. This is the key to the system's idempotency.

## Execution

The pipelines are designed to be run sequentially. They can be executed directly or, more conveniently, via the provided `pipeline_validation.ipynb` notebook.

1.  **Run Ingestion**:
    ```bash
    python -m pipelines.ingest_statements
    ```
2.  **Run Transformation**:
    ```bash
    python -m pipelines.transform_statements
    ```

## Validation

The functionality and idempotency of these pipelines can be interactively verified by running the `pipeline_validation.ipynb` notebook located in the root of this project. The notebook provides a step-by-step walkthrough of the entire process.

This architecture ensures that each component is doing a single job well, providing a robust and scalable foundation for the entire credit scoring system. 
