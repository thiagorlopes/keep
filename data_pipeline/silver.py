import pandas as pd
from pathlib import Path

def clean_bronze_data(bronze_path, silver_path):
    """
    Reads data from the bronze layer, cleans it, and saves it to the silver layer.

    Args:
        bronze_path (str or Path): The path to the bronze layer directory.
        silver_path (str or Path): The path to the silver layer directory.
    """
    bronze_path = Path(bronze_path)
    silver_path = Path(silver_path)

    # Ensure the silver directory exists
    silver_path.mkdir(parents=True, exist_ok=True)

    # Iterate over each account's data in the bronze layer
    for account_dir in bronze_path.iterdir():
        if account_dir.is_dir():
            bronze_file = account_dir / 'data.parquet'
            if bronze_file.exists():
                try:
                    df = pd.read_parquet(bronze_file)

                    # --- Data Cleaning ---
                    for col in df.columns:
                        # Check if the column has a numeric type
                        if pd.api.types.is_numeric_dtype(df[col]):
                            df[col] = df[col].fillna(0)
                        # Check if the column has an object type (likely strings)
                        elif pd.api.types.is_object_dtype(df[col]):
                            df[col] = df[col].fillna('')

                    # Define the output path in the silver layer
                    silver_account_dir = silver_path / account_dir.name
                    silver_account_dir.mkdir(parents=True, exist_ok=True)
                    silver_file = silver_account_dir / 'data.parquet'

                    # Save the cleaned data to the silver layer
                    df.to_parquet(silver_file, engine='fastparquet')

                    print(f"Cleaned data for account {account_dir.name} and saved to {silver_file}")

                except Exception as e:
                    print(f"Error processing file {bronze_file}: {e}")

def main():
    """Main function to run the silver data processing."""
    BRONZE_PATH = 'data_lake/bronze'
    SILVER_PATH = 'data_lake/silver'

    clean_bronze_data(BRONZE_PATH, SILVER_PATH)

if __name__ == "__main__":
    main()
