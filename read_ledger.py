from deltalake import DeltaTable
from pathlib import Path

# Define the path to your ledger, now inside the pipelines directory
LEDGER_PATH = Path(__file__).parent / 'pipelines/data_lake/application_status_ledger'

try:
    # Load the Delta table
    dt = DeltaTable(LEDGER_PATH)
    
    # Convert it to a pandas DataFrame for easy viewing
    df = dt.to_pandas()
    
    print(f"Contents of the ledger at: {LEDGER_PATH}")
    print("=" * 50)
    print(df)
    print("=" * 50)
    print(f"Found {len(df)} record(s).")

except Exception as e:
    print(f"Error reading Delta table at {LEDGER_PATH}: {e}")
    print("Please ensure the pipeline has been run at least once to create the ledger.") 
