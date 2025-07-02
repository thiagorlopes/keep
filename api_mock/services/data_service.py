import csv
import os
import json
import logging
from typing import Any, Dict, List

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config.json')

def _load_config():
    """Loads configuration from config.json."""
    if not os.path.exists(CONFIG_PATH):
        logging.error(f"Config file not found at {CONFIG_PATH}")
        return {"accounts": []}
    try:
        with open(CONFIG_PATH, 'r') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from {CONFIG_PATH}: {e}")
        return {"accounts": []}

_config = _load_config()
ACCOUNTS = _config.get("accounts", [])
STATEMENTS_MAPPING = {
    account['Id']: account['statementFile']
    for account in ACCOUNTS
    if 'Id' in account and 'statementFile' in account
}

if not ACCOUNTS:
    logging.warning("No accounts loaded from configuration.")

def _get_transaction_type(amount: float) -> str:
    """Determines transaction type based on amount."""
    return 'credit' if amount > 0 else 'debit'

CREDIT_MULTIPLIER = 1
DEBIT_MULTIPLIER = -1

_AMOUNT_FIELD_MAPPING = {
    'deposits': CREDIT_MULTIPLIER,
    'credit': CREDIT_MULTIPLIER,
    'withdrawals': DEBIT_MULTIPLIER,
    'debit': DEBIT_MULTIPLIER,
}

def _parse_amount(row_data: Dict[str, str]) -> float:
    """Parses transaction amount from row data using a predefined mapping."""
    for field, sign in _AMOUNT_FIELD_MAPPING.items():
        if field in row_data and row_data[field]:
            amount_str = row_data[field].replace(',', '')
            return float(amount_str) * sign
    return 0.0

def load_transactions(account_id: str) -> List[Dict[str, Any]]:
    """
    Loads and processes transactions for the mock Flinks API.
    Returns a list of processed transaction dictionaries.
    """
    transactions: List[Dict[str, Any]] = []
    filename = STATEMENTS_MAPPING.get(account_id)
    if not filename:
        logging.warning(f"No statement file mapping found for account_id: {account_id}")
        return transactions

    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        logging.error(f"Transaction file not found: {filepath}")
        return transactions

    logging.info(f"Loading and processing transactions for account {account_id} from {filename}")
    with open(filepath, mode='r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            row_data_lower = {k.lower().strip(): v for k, v in row.items()}
            amount = _parse_amount(row_data_lower)

            transaction = {
                # Use original column names from the CSV for the mock API response
                'Date': row.get('Date', ''),
                'Description': row.get('Description', ''),
                'Amount': amount,
                'Type': _get_transaction_type(amount),
                'Balance': row.get('Balance', '0'),
                 # Keep other original fields as-is if they exist
                **row
            }
            transactions.append(transaction)

    logging.info(f"Successfully loaded and processed {len(transactions)} transactions for account {account_id}.")
    return transactions

def load_raw_transactions(account_id: str) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Loads raw, unprocessed transactions and headers from a CSV file.
    Returns a tuple containing the list of rows and the original headers.
    """
    transactions: List[Dict[str, Any]] = []
    headers: List[str] = []
    filename = STATEMENTS_MAPPING.get(account_id)
    if not filename:
        logging.warning(f"No statement file mapping found for account_id: {account_id}")
        return transactions, headers

    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        logging.error(f"Transaction file not found: {filepath}")
        return transactions, headers

    logging.info(f"Loading raw transactions for account {account_id} from {filename}")
    with open(filepath, mode='r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        headers = list(reader.fieldnames) if reader.fieldnames else []
        transactions = list(reader)

    logging.info(f"Successfully loaded {len(transactions)} raw transactions for account {account_id}.")
    return transactions, headers

def get_accounts() -> List[Dict[str, Any]]:
    """Returns the list of accounts from the config."""
    return ACCOUNTS

def is_valid_account(account_id: str) -> bool:
    """Checks if the account ID is valid."""
    return account_id in STATEMENTS_MAPPING

def get_account_id_by_number(account_number: str) -> str | None:
    """Finds an account by its account number and returns its internal ID."""
    for account in ACCOUNTS:
        if account.get('AccountNumber') == account_number:
            return account.get('Id')
    return None

def get_account_ids_by_email(email: str) -> list[str]:
    """Finds all account IDs associated with a given email."""
    if not email:
        return []

    logging.info(f"Searching for accounts with email: {email}")
    found_ids = [
        account['Id']
        for account in ACCOUNTS
        if account.get('Holder', {}).get('Email', '').lower() == email.lower()
    ]
    logging.info(f"Found {len(found_ids)} account(s) for email: {email}")
    return found_ids
