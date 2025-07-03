import pytest
import json
from uuid import uuid4
from api_mock import create_app
from api_mock.services import data_service

@pytest.fixture
def client():
    """Create and configure a test client for the app."""
    app = create_app()
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

def test_authorize_endpoint(client):
    """Test the /Authorize endpoint."""
    customer_id = uuid4()
    response = client.post(f'/v3/{customer_id}/BankingServices/Authorize')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert 'LoginId' in data
    assert 'RequestId' in data
    assert data['StatusCode'] == 200

def test_get_accounts_detail_endpoint(client):
    """Test the /GetAccountsDetail endpoint."""
    customer_id = uuid4()
    login_id = uuid4()
    response = client.post(
        f'/v3/{customer_id}/BankingServices/GetAccountsDetail',
        data=json.dumps({'LoginId': str(login_id)}),
        content_type='application/json'
    )
    assert response.status_code == 200
    data = json.loads(response.data)
    assert 'Accounts' in data
    assert len(data['Accounts']) == 2
    assert data['Login']['Id'] == str(login_id)

def test_get_statements_endpoint_success(client, monkeypatch):
    """Test the /GetStatements endpoint for a successful case."""
    # We can mock the service layer to isolate the API test
    monkeypatch.setattr(
        'api_mock.services.data_service.load_transactions',
        lambda x: [{'Amount': 100}]
    )

    customer_id = uuid4()
    login_id = uuid4()
    # Use a valid account number from the data service
    account_number = data_service.ACCOUNTS[0]['AccountNumber']

    response = client.post(
        f'/v3/{customer_id}/BankingServices/GetStatements',
        data=json.dumps({'LoginId': str(login_id), 'AccountNumber': account_number}),
        content_type='application/json'
    )
    assert response.status_code == 200
    data = json.loads(response.data)
    # The API returns the internal ID, not the account number
    assert data['Statements'][0]['AccountId'] == data_service.ACCOUNTS[0]['Id']
    assert len(data['Statements'][0]['Transactions']) == 1

def test_get_statements_invalid_account(client):
    """Test the /GetStatements endpoint with an invalid AccountId."""
    customer_id = uuid4()
    login_id = uuid4()
    response = client.post(
        f'/v3/{customer_id}/BankingServices/GetStatements',
        data=json.dumps({'LoginId': str(login_id), 'AccountNumber': 'invalid_account_number'}),
        content_type='application/json'
    )
    assert response.status_code == 404
    data = json.loads(response.data)
    assert "not found" in data['error']
