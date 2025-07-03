import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from deltalake import DeltaTable
import os
import requests

# Import the functions to be tested
from pipelines.ingest_statements import fetch_data_from_api, main as ingest_main
from pipelines.transform_statements import main as transform_main

@pytest.fixture
def mock_api_success_response():
    """Provides a sample successful API response."""
    return {
        "Statements": [{
            "Transactions": [
                {'Date': '2023-01-15', 'Description': 'Payment', 'Amount': -100.0, 'Balance': 4900.0},
                {'Date': '2023-01-10', 'Description': 'Deposit', 'Amount': 200.0, 'Balance': 5000.0}
            ]
        }]
    }

# --- Tests for ingest_statements.py ---

@patch('requests.post')
def test_fetch_data_from_api_success(mock_post, mock_api_success_response):
    """Test successful API data fetching."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_api_success_response
    mock_post.return_value = mock_response

    result = fetch_data_from_api("c_id", "l_id", "acc_num")

    assert result == mock_api_success_response
    mock_post.assert_called_once()

@patch('requests.post')
def test_fetch_data_from_api_failure(mock_post):
    """Test API data fetching failure."""
    mock_post.side_effect = requests.exceptions.RequestException("API is down")
    
    result = fetch_data_from_api("c_id", "l_id", "acc_num")
    
    assert result is None

@patch('pipelines.ingest_statements.fetch_data_from_api')
@patch('builtins.open')
@patch('pipelines.ingest_statements.write_deltalake')
def test_ingest_main(mock_write_deltalake, mock_open, mock_fetch, mock_api_success_response):
    """Test the main ingestion pipeline end-to-end."""
    # Setup: Mock config.json and the API response
    mock_fetch.return_value = mock_api_success_response
    mock_open.return_value.__enter__.return_value.read.return_value = '{"accounts": [{"Id": "acc_1", "AccountNumber": "123"}]}'
    
    ingest_main()

    # Verification
    # Check that write_deltalake was called with the correct data
    mock_write_deltalake.assert_called_once()
    call_args, call_kwargs = mock_write_deltalake.call_args
    
    written_df = call_args[1]
    assert len(written_df) == 2
    assert 'account_id' in written_df.columns
    assert written_df['account_id'].iloc[0] == 'acc_1'
    assert written_df['description'].iloc[0] == 'Payment'
    assert call_kwargs['mode'] == 'append'

# --- Tests for transform_statements.py ---

@pytest.fixture
def bronze_table_path(tmp_path):
    """Creates a sample bronze delta table and returns the root data lake path."""
    data_lake_root = tmp_path / "data_lake_root"
    bronze_path = data_lake_root / "data_lake" / "bronze"
    os.makedirs(bronze_path)
    
    data = [
        {'date': '2023-01-15', 'description': 'p1', 'amount': 100.0, 'email': 'a@a.com', 'request_id': 'r1', 'extra_col': 'foo', 'numeric_col': 1.0},
        {'date': '2023-01-16', 'description': 'p2', 'amount': None, 'email': 'b@b.com', 'request_id': 'r2', 'extra_col': None, 'numeric_col': None}
    ]
    df = pd.DataFrame(data)
    
    from deltalake.writer import write_deltalake
    write_deltalake(str(bronze_path), df, mode='overwrite')
    return str(data_lake_root)

def test_transform_main_overwrite(bronze_table_path):
    """Test the main transform pipeline in overwrite mode."""
    silver_path = os.path.join(bronze_table_path, "data_lake/silver")
    ledger_path = os.path.join(bronze_table_path, "data_lake/application_status_ledger")

    transform_main(write_mode='overwrite', data_lake_root=bronze_table_path)

    # --- Verification ---
    assert os.path.exists(silver_path)
    silver_df = DeltaTable(silver_path).to_pandas()
    assert len(silver_df) == 2
    assert silver_df['amount'].iloc[1] == 0
    assert silver_df['extra_col'].iloc[1] == ''
    
    assert os.path.exists(ledger_path)
    ledger_df = DeltaTable(ledger_path).to_pandas()
    assert len(ledger_df) == 2
    assert 'status' in ledger_df.columns
    assert ledger_df['status'].iloc[0] == 'PENDING'

def test_transform_main_merge(bronze_table_path):
    """Test the main transform pipeline in merge mode."""
    silver_path = os.path.join(bronze_table_path, "data_lake/silver")
    
    # First run to create the table
    transform_main(write_mode='overwrite', data_lake_root=bronze_table_path)
    # Second run with the same data (should not insert duplicates)
    transform_main(write_mode='merge', data_lake_root=bronze_table_path)
    
    silver_df = DeltaTable(silver_path).to_pandas()
    assert len(silver_df) == 2
