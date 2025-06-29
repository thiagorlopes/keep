import os
import json
import requests
import pandas as pd
from pathlib import Path
from deltalake.writer import write_deltalake

def fetch_data_from_api(customer_id, login_id, account_number, api_url_template="http://127.0.0.1:5000/v3/{customer_id}/BankingServices/GetStatements"):
    """
    Fetches data from the GetStatements API for a given customer and account.

    Args:
        customer_id (str): The customer's UUID.
        login_id (str): The login ID obtained from the authorization step.
        account_number (str): The account number to fetch statements for.
        api_url_template (str): The URL template for the API endpoint.

    Returns:
        dict: The JSON response from the API, or None if the request fails.
    """
    api_url = api_url_template.format(customer_id=customer_id)
    payload = {
        "LoginId": login_id,
        "AccountNumber": account_number
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

def main():
    """Main function to run the bronze ingestion pipeline."""
    # Define the data lake path at the project root
    BRONZE_PATH = 'data_lake/bronze'
    CONFIG_PATH = 'config.json'

    # Load account information from config.json
    try:
        with open(CONFIG_PATH, 'r') as f:
            config = json.load(f)
        accounts_to_process = config.get("accounts", [])
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading or parsing {CONFIG_PATH}: {e}")
        return

    customer_id = "123e4567-e89b-12d3-a456-426614174000"
    login_id = "abc-123"

    all_transactions = []

    for account in accounts_to_process:
        account_id = account.get("Id")
        account_number = account.get("AccountNumber")
        if not all([account_id, account_number]):
            print(f"Skipping account due to missing 'Id' or 'AccountNumber': {account}")
            continue

        print(f"Fetching data for account number: {account_number} (Id: {account_id})")
        api_data = fetch_data_from_api(customer_id, login_id, account_number)

        if api_data and 'Statements' in api_data:
            statements = api_data.get('Statements', [])
            for statement in statements:
                transactions = statement.get('Transactions', [])
                if transactions:
                    df = pd.DataFrame(transactions)
                    # Add account_id to each transaction for tracking
                    df['account_id'] = account_id
                    all_transactions.append(df)

    if all_transactions:
        # Combine all transactions into a single DataFrame
        final_df = pd.concat(all_transactions, ignore_index=True)

        # Standardize column names
        final_df.columns = [col.replace(' ', '_').replace('/', '_').lower() for col in final_df.columns]

        # Write to a single bronze Delta table, appending new data
        print(f"Writing {len(final_df)} transactions to bronze Delta table at {BRONZE_PATH}...")
        write_deltalake(BRONZE_PATH, final_df, mode='append')
        print("Bronze ingestion complete.")
    else:
        print("No transactions were fetched to ingest.")

if __name__ == "__main__":
    main()
