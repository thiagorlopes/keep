import pytest
from unittest.mock import mock_open, patch
from api_mock.services import data_service

@pytest.fixture
def mock_csv_data():
    """Fixture to provide mock CSV data for testing."""
    return "Date,Description,Withdrawals,Deposits,Balance\n" \
           "2023-01-15,Payment,100.00,,4900.00\n" \
           "2023-01-10,Deposit,,200.00,5000.00\n"

def test_load_transactions_success(monkeypatch, mock_csv_data):
    """Test successful loading and parsing of transactions."""
    mock_file = mock_open(read_data=mock_csv_data)
    monkeypatch.setattr("builtins.open", mock_file)
    monkeypatch.setattr("os.path.exists", lambda _: True)

    transactions = data_service.load_transactions('001_statement_a')

    assert len(transactions) == 2
    assert transactions[0]['Description'] == 'Payment'
    assert transactions[0]['Amount'] == -100.0
    assert transactions[0]['Type'] == 'debit'
    assert transactions[1]['Description'] == 'Deposit'
    assert transactions[1]['Amount'] == 200.0
    assert transactions[1]['Type'] == 'credit'

def test_load_transactions_invalid_account():
    """Test that loading transactions for an invalid account returns an empty list."""
    transactions = data_service.load_transactions('invalid_id')
    assert transactions == []

def test_load_transactions_file_not_found(monkeypatch):
    """Test that loading transactions returns an empty list if the file doesn't exist."""
    monkeypatch.setattr("os.path.exists", lambda _: False)
    transactions = data_service.load_transactions('001_statement_a')
    assert transactions == []

def test_get_accounts():
    """Test that get_accounts returns the predefined list of accounts."""
    accounts = data_service.get_accounts()
    assert accounts == data_service.ACCOUNTS
    assert len(accounts) == 2

def test_is_valid_account():
    """Test the account ID validation logic."""
    assert data_service.is_valid_account('001_statement_a') is True
    assert data_service.is_valid_account('002_statement_b') is True
    assert data_service.is_valid_account('non_existent_id') is False

def test_get_account_ids_by_email():
    """Test retrieving account IDs by email."""
    # Test with an email that has two accounts from the config
    assert data_service.get_account_ids_by_email("joelschaubel@gmail.com") == ["001_statement_a", "002_statement_b"]
    # Test with a non-existent email
    assert data_service.get_account_ids_by_email("nonexistent@email.com") == []

def test_get_account_id_by_number():
    """Test retrieving account ID by account number."""
    # Test with a valid account number
    account_number = data_service.ACCOUNTS[0]['AccountNumber']
    expected_id = data_service.ACCOUNTS[0]['Id']
    assert data_service.get_account_id_by_number(account_number) == expected_id
    # Test with a non-existent account number
    assert data_service.get_account_id_by_number("999999999") is None

def test_load_raw_transactions(monkeypatch):
    """Test loading raw transaction data."""
    # This test doesn't need to mock the file system, as it should find the actual mock data
    transactions, headers = data_service.load_raw_transactions('001_statement_a')
    assert len(transactions) > 0
    assert "Date" in headers
    assert "Description" in headers

    # Test file not found case for a non-existent account
    transactions, headers = data_service.load_raw_transactions('non_existent_id')
    assert transactions == []
    assert headers == []
