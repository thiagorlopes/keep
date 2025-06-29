import pandas as pd
from deltalake import DeltaTable, write_deltalake
from pathlib import Path
import os
import argparse

# Use an environment variable to determine the root path, defaulting to a relative path for local execution.
DATA_LAKE_ROOT = os.getenv("DATA_LAKE_ROOT", ".")

def main(write_mode: str):
    """
    Reads new data from the bronze layer, cleans it, and writes it to the
    silver layer and the scoring_status ledger. This pipeline is fully
    idempotent and scalable.
    """
    BRONZE_PATH = os.path.join(DATA_LAKE_ROOT, 'data_lake/bronze')
    SILVER_PATH = os.path.join(DATA_LAKE_ROOT, 'data_lake/silver')
    STATUS_LEDGER_PATH = os.path.join(DATA_LAKE_ROOT, 'data_lake/application_status_ledger')

    # Read the entire bronze table.
    try:
        df = DeltaTable(BRONZE_PATH).to_pandas()
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

    if 'email' not in df.columns or 'request_id' not in df.columns:
        print("Error: 'email' or 'request_id' not found in bronze data.")
        return

    print(f"Writing {len(df)} cleaned rows to the silver layer with mode: {write_mode}...")
    if write_mode == 'overwrite':
        write_deltalake(SILVER_PATH, df, mode="overwrite", schema_mode="overwrite") # type: ignore
    else: # Default to merge for safety
        try:
            (
                DeltaTable(SILVER_PATH)
                .merge(
                    source=df, # type: ignore
                    predicate="target.email = source.email AND target.request_id = source.request_id AND target.date = source.date AND target.description = source.description",
                    source_alias="source",
                    target_alias="target"
                )
                .when_not_matched_insert_all()
                .execute()
            )
        except Exception:
            print("Silver table not found, creating new one.")
            write_deltalake(SILVER_PATH, df, mode="overwrite", schema_mode="overwrite") # type: ignore
    print("Silver layer write complete.")

    # --- Update the Scoring Status Ledger ---
    status_df = df[['email', 'request_id']].drop_duplicates().copy()
    status_df['status'] = 'PENDING'
    print(f"Writing {len(status_df)} records to scoring status ledger with mode: {write_mode}...")
    
    if write_mode == 'overwrite':
        write_deltalake(STATUS_LEDGER_PATH, status_df, mode="overwrite", schema_mode="overwrite") # type: ignore
    else: # Default to merge for safety
        try:
            (
                DeltaTable(STATUS_LEDGER_PATH)
                .merge(
                    source=status_df, # type: ignore
                    predicate="target.email = source.email AND target.request_id = source.request_id",
                    source_alias="source",
                    target_alias="target"
                )
                .when_not_matched_insert_all()
                .execute()
            )
        except Exception:
            print("Ledger not found, creating new one.")
            write_deltalake(STATUS_LEDGER_PATH, status_df, mode="overwrite", schema_mode="overwrite") # type: ignore
    print("Ledger write complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--write-mode",
        type=str,
        default="merge",
        choices=['merge', 'overwrite'],
        help="The write mode for the pipeline (merge or overwrite)."
    )
    args = parser.parse_args()
    main(write_mode=args.write_mode)
