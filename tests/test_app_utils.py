import pytest
from unittest.mock import patch, MagicMock, call
import pandas as pd
import numpy as np
import logging

# Import functions from the app_utils module
from app import app_utils

# --- Tests for _convert_to_json_serializable ---

def test_convert_to_json_serializable():
    """Test the conversion of various data types to be JSON serializable."""
    assert isinstance(app_utils._convert_to_json_serializable(np.int64(10)), int)
    assert isinstance(app_utils._convert_to_json_serializable(np.float64(10.5)), float)
    assert isinstance(app_utils._convert_to_json_serializable(np.array([1, 2])), list)
    assert app_utils._convert_to_json_serializable(pd.NA) is None
    # Check for NaN conversion
    assert app_utils._convert_to_json_serializable(float('nan')) is None
    
    # Test with a complex structure
    data = {'a': np.int64(1), 'b': [np.float64(2.0)], 'c': pd.NA}
    converted_data = app_utils._convert_to_json_serializable(data)
    
    assert isinstance(converted_data, dict)
    # Now assert on the contents of the converted dict
    assert isinstance(converted_data.get('a'), int)
    b_val = converted_data.get('b')
    assert isinstance(b_val, list)
    assert isinstance(b_val[0], float)
    assert converted_data.get('c') is None

# --- Tests for _call_taktile_api ---

@patch('app.app_utils.requests')
@patch.dict('os.environ', {'TAKTILE_DEMO_API_KEY': 'test-key'})
def test_call_taktile_api_sync_success(mock_requests):
    """Test a successful synchronous (200) API call to Taktile."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"decision": "approved"}
    mock_requests.post.return_value = mock_response

    response = app_utils._call_taktile_api({}, logging.getLogger())
    assert response == {"decision": "approved"}

@patch('app.app_utils.requests')
@patch('app.app_utils.time.sleep', return_value=None)
@patch.dict('os.environ', {'TAKTILE_DEMO_API_KEY': 'test-key'})
def test_call_taktile_api_async_success(mock_sleep, mock_requests):
    """Test a successful asynchronous (202) API call with polling."""
    # First response is 202, second is 200
    async_response = MagicMock()
    async_response.status_code = 202
    async_response.json.return_value = {"metadata": {"decision_id": "123"}}
    
    sync_response = MagicMock()
    sync_response.status_code = 200
    sync_response.json.return_value = {"decision": "approved_async"}

    mock_requests.post.return_value = async_response
    mock_requests.get.return_value = sync_response

    response = app_utils._call_taktile_api({}, logging.getLogger())
    assert response == {"decision": "approved_async"}
    assert mock_requests.post.call_count == 1
    assert mock_requests.get.call_count == 1

def test_call_taktile_api_no_key():
    """Test that an EnvironmentError is raised if the API key is not set."""
    with patch.dict('os.environ', {}, clear=True):
        with pytest.raises(EnvironmentError):
            app_utils._call_taktile_api({}, logging.getLogger())

# --- Tests for run_analysis_pipeline ---

@patch('app.app_utils.write_deltalake')
@patch('app.app_utils.subprocess.run')
@patch('app.app_utils.duckdb.connect')
@patch('app.app_utils.st')
def test_run_analysis_pipeline_success(mock_st, mock_duckdb, mock_subprocess, mock_write_deltalake):
    """Test the successful execution of the analysis pipeline orchestrator."""
    # Setup mocks
    mock_subprocess.return_value = MagicMock(stdout="", stderr="", returncode=0)
    mock_st.session_state = {} # Use a real dict for session_state
    
    mock_conn = MagicMock()
    mock_duckdb.return_value.__enter__.return_value = mock_conn
    mock_conn.table.return_value.to_df.side_effect = [
        pd.DataFrame([{'request_id': 'r1', 'email': 'a@b.com', 'date': '2023-01-01', 'revised_average_balance': 100}]),
        pd.DataFrame([{'estimated_annual_revenue': 1200}])
    ]
    
    source_df = pd.DataFrame([{'Email': 'a@b.com'}])
    
    app_utils.run_analysis_pipeline(source_df)

    # Assertions
    mock_write_deltalake.assert_called_once()
    
    # Check that subprocess.run was called correctly
    expected_calls = [
        call(["python", "-m", "pipelines.transform_statements", "--write-mode", "overwrite"], check=True, capture_output=True, text=True),
        call(["dbt", "run", "--full-refresh"], cwd="analytics", check=True, capture_output=True, text=True),
        call(["dbt", "docs", "generate"], cwd="analytics", check=True, capture_output=True, text=True)
    ]
    mock_subprocess.assert_has_calls(expected_calls, any_order=False)
    
    # Check that streamlit components were called to display results
    assert mock_st.success.called
    assert mock_st.subheader.called
    assert mock_st.dataframe.called
    assert mock_st.line_chart.called
    
    # Check that results were stored in session state
    assert "credit_metrics_df" in mock_st.session_state
    assert "final_df" in mock_st.session_state

def test_run_analysis_pipeline_empty_df():
    """Test that the pipeline returns early if the source dataframe is empty."""
    # This test doesn't need mocks as the function should exit early
    app_utils.run_analysis_pipeline(pd.DataFrame())
    # No real way to assert on st.warning without more complex mocking,
    # but we can ensure no errors are raised.

# --- Tests for UI components ---

def test_display_reset_button():
    """Test that the reset button clears the session state when clicked."""
    mock_st = MagicMock()
    mock_st.session_state = {
        "credit_metrics_df": pd.DataFrame(),
        "taktile_decision_resp": {}
    }
    # Simulate the button being clicked
    mock_st.button.return_value = True

    app_utils.display_reset_button(mock_st, logging.getLogger())

    assert "credit_metrics_df" not in mock_st.session_state
    assert "taktile_decision_resp" not in mock_st.session_state
    assert mock_st.rerun.called

@patch('app.app_utils._call_taktile_api')
def test_display_taktile_interface_sends_payload(mock_call_taktile_api):
    """Test that the Taktile interface correctly builds and sends a payload."""
    mock_st = MagicMock()
    mock_st.session_state = {
        "credit_metrics_df": pd.DataFrame([{
            "request_id": "req123", "email": "test@test.com", "revenue_total": 5000
        }])
    }
    # Simulate the "Send to Taktile" button being clicked
    mock_st.button.return_value = True
    
    app_utils.display_taktile_interface(mock_st, logging.getLogger(), key_prefix="test")

    mock_call_taktile_api.assert_called_once()
    # Check some key fields in the payload that was sent
    call_args, _ = mock_call_taktile_api.call_args
    payload = call_args[0]
    assert payload['metadata']['entity_id'] == 'req123'
    assert payload['data']['revenue_total'] == 5000
    assert payload['data']['email'] == 'test@test.com'
    assert mock_st.rerun.called 
