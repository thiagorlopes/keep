import pandas as pd
from deltalake import DeltaTable, write_deltalake
from pathlib import Path

def main():
    """
    Reads new data from the bronze layer, cleans it, and merges it into the
    silver layer and the scoring_status ledger. This pipeline is fully
    idempotent and scalable.
    """
    BRONZE_PATH = 'data_lake/bronze'
    SILVER_PATH = 'data_lake/silver'
    STATUS_LEDGER_PATH = 'data_lake/application_status_ledger'

    # Read the entire bronze table. In a production scenario with large data,
    # this would be optimized with streaming reads or incremental processing.
    try:
        bronze_dt = DeltaTable(BRONZE_PATH)
        df = bronze_dt.to_pandas()
    except Exception as e:
        print(f"Error reading bronze Delta table at {BRONZE_PATH}: {e}")
        return

    print(f"Read {len(df)} rows from the bronze layer.")

    # --- Data Cleaning ---
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(0)
        elif pd.api.types.is_object_dtype(df[col]):
            df[col] = df[col].fillna('')

    # Ensure key columns exist for the ledger merge
    if 'email' not in df.columns or 'request_id' not in df.columns:
        print("Error: 'email' or 'request_id' not found in bronze data.")
        return

    # --- Upsert into Silver Layer ---
    print(f"Merging {len(df)} cleaned rows into the silver Delta table...")
    try:
        (
            DeltaTable(SILVER_PATH)
            .merge(
                source=df,
                # Define the merge condition to avoid duplicates
                predicate="target.email = source.email AND target.request_id = source.request_id AND target.date = source.date AND target.description = source.description",
                source_alias="source",
                target_alias="target"
            )
            .when_not_matched_insert_all()
            .execute()
        )
    except Exception:
        print("Silver table not found, creating new one.")
        write_deltalake(SILVER_PATH, df)

    print("Silver layer merge complete.")

    # --- Idempotently Update the Scoring Status Ledger ---
    print("Updating scoring status ledger...")
    status_df = df[['email', 'request_id']].drop_duplicates().copy()
    status_df['status'] = 'PENDING'

    try:
        # If the table exists, merge. Otherwise, create it.
        (
            DeltaTable(STATUS_LEDGER_PATH)
            .merge(
                source=status_df,
                predicate="target.email = source.email AND target.request_id = source.request_id",
                source_alias="source",
                target_alias="target"
            )
            .when_not_matched_insert_all()
            .execute()
        )
        print(f"Ledger merge complete. {len(status_df)} records processed.")
    except Exception:
        print("Ledger not found, creating new one.")
        write_deltalake(STATUS_LEDGER_PATH, status_df)
        print("New ledger created.")


if __name__ == "__main__":
    main()
