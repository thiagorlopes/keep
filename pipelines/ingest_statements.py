import os
import requests
import pandas as pd
from pathlib import Path

def fetch_data_from_api(customer_id, login_id, account_id, api_url_template="http://127.0.0.1:5000/v3/{customer_id}/BankingServices/GetStatements"):
    """
    Fetches data from the GetStatements API for a given customer and account.

    Args:
        customer_id (str): The customer's UUID.
        login_id (str): The login ID obtained from the authorization step.
        account_id (str): The account ID to fetch statements for.
        api_url_template (str): The URL template for the API endpoint.

    Returns:
        dict: The JSON response from the API, or None if the request fails.
    """
    api_url = api_url_template.format(customer_id=customer_id)
    payload = {
        "LoginId": login_id,
        "AccountId": account_id
    }
    headers = {
        "Content-Type": "application/json"
    }
    try:
        response = requests.post(api_url, json=payload, headers=headers)
        response.raise_for_status()  # Raises an exception for 4XX or 5XX status codes
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None

def process_and_save_statements(api_response, bronze_path):
    """
    Processes the API response, extracts transactions, and saves them as Parquet files.

    Args:
        api_response (dict): The JSON response from the API.
        bronze_path (str or Path): The path to the bronze layer directory.
    """
    if not api_response or 'Statements' not in api_response:
        print("No statements found in the API response.")
        return

    statements = api_response.get('Statements', [])
    for statement in statements:
        account_id = statement.get('AccountId')
        transactions = statement.get('Transactions', [])

        if not all([account_id, transactions]):
            print("Skipping statement due to missing account ID or transactions.")
            continue

        try:
            # Load transactions into a pandas DataFrame
            df = pd.DataFrame(transactions)

            # --- Data Cleaning and Schema Definition ---
            # Standardize column names (e.g., to snake_case)
            df.columns = [col.replace(' ', '_').replace('/', '_').lower() for col in df.columns]

            # Define a basic schema and data types
            # Based on the new JSON structure
            schema = {
                'username': 'string',
                'email': 'string',
                'address': 'string',
                'financial_institution': 'string',
                'employer_name': 'string',
                'login_id': 'string',
                'request_id': 'string',
                'request_date_time': 'datetime64[ns]',
                'request_status': 'string',
                'days_detected': 'string',
                'tag': 'string',
                'account_name': 'string',
                'account_number': 'string',
                'account_type': 'string',
                'account_balance': 'float64',
                'date': 'datetime64[ns]',
                'description': 'string',
                'category': 'string',
                'subcategory': 'string',
                'withdrawals': 'float64',
                'deposits': 'float64',
                'balance': 'float64',
                'amount': 'float64',
                'type': 'string'
            }

            # Ensure all columns exist, fill missing with None
            for col in schema:
                if col not in df.columns:
                    df[col] = None

            # Convert numeric columns, coercing errors
            for col in ['withdrawals', 'deposits', 'balance', 'account_balance']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')


            # Apply the schema
            df = df.astype({k: v for k, v in schema.items() if k in df.columns and 'datetime' not in v and k in df.columns})

            # Handle datetimes separately
            for col, type in schema.items():
                if type == 'datetime64[ns]' and col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

            # Create a directory for the account if it doesn't exist
            output_dir = Path(bronze_path) / account_id
            output_dir.mkdir(parents=True, exist_ok=True)

            # Save as Parquet
            output_file = output_dir / 'data.parquet'
            df.to_parquet(output_file, engine='fastparquet', index=False)

            print(f"Saved statement for account {account_id} to {output_file}")

        except Exception as e:
            print(f"An error occurred while processing account {account_id}: {e}")


def main():
    """Main function to run the bronze ingestion pipeline."""
    BRONZE_PATH = 'data_lake/bronze'

    Path(BRONZE_PATH).mkdir(parents=True, exist_ok=True)

    # In a real app, these would come from a config or an orchestration tool
    customer_id = "123e4567-e89b-12d3-a456-426614174000"
    login_id = "abc-123"
    accounts_to_process = ["001_statement_a", "002_statement_b"]

    for account_id in accounts_to_process:
        print(f"Fetching data for account: {account_id}")
        api_data = fetch_data_from_api(customer_id, login_id, account_id)

        if api_data:
            process_and_save_statements(api_data, BRONZE_PATH)

if __name__ == "__main__":
    main()
