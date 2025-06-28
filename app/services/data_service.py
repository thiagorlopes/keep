import csv
import os
from typing import Any, Dict, List

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'data')

STATEMENTS_MAPPING = {
    '001_statement_a': '001_statement_a.csv',
    '002_statement_b': '002_statement_b.csv',
}

ACCOUNTS = [
    {
        'Id': '001_statement_a',
        'AccountNumber': '010-30800-0095971396',
        'Type': 'Operation',
        'Balance': {'Current': 2016.70},
        'Holder': {'Name': 'Joel Schaubel', 'Email': 'joelschaubel@gmail.com'},
        'Institution': 'Simplii',
    },
    {
        'Id': '002_statement_b',
        'AccountNumber': '010-30800-0095983938',
        'Type': 'Operation',
        'Balance': {'Current': 418.52},
        'Holder': {'Name': 'Joel Schaubel', 'Email': 'joelschaubel@gmail.com'},
        'Institution': 'Simplii',
    },
]

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
    """Loads transactions from a CSV file for a given account ID."""
    transactions: List[Dict[str, Any]] = []
    filename = STATEMENTS_MAPPING.get(account_id)
    if not filename:
        return transactions

    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        return transactions

    with open(filepath, mode='r', encoding='utf-8') as infile:
        # Use DictReader for more robust column handling
        reader = csv.DictReader(infile)

        for row in reader:
            # Standardize keys to lower case for consistency
            row_data = {k.lower().strip(): v for k, v in row.items()}

            # Skip initial balance or empty rows if necessary
            if "balance" in row_data.get('description', '').lower() or not row_data.get('description'):
                continue

            amount = _parse_amount(row_data)

            transaction = {
                'Date': row_data.get('date', ''),
                'Description': row_data.get('description', ''),
                'Amount': amount,
                'Type': _get_transaction_type(amount),
                'Balance': float(row_data.get('balance', '0').replace(',', '')),
            }
            transactions.append(transaction)

    return transactions

def get_accounts() -> List[Dict[str, Any]]:
    """Returns the list of accounts."""
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
    return [
        account['Id']
        for account in ACCOUNTS
        if account.get('Holder', {}).get('Email', '').lower() == email.lower()
    ]
