import pytest
import json
from uuid import uuid4
from app import create_app

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
        'app.services.data_service.load_transactions',
        lambda x: [{'Amount': 100}]
    )

    customer_id = uuid4()
    login_id = uuid4()
    account_id = '001_statement_a'

    response = client.post(
        f'/v3/{customer_id}/BankingServices/GetStatements',
        data=json.dumps({'LoginId': str(login_id), 'AccountId': account_id}),
        content_type='application/json'
    )
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data['Statements'][0]['AccountId'] == account_id
    assert len(data['Statements'][0]['Transactions']) == 1

def test_get_statements_invalid_account(client):
    """Test the /GetStatements endpoint with an invalid AccountId."""
    customer_id = uuid4()
    login_id = uuid4()
    response = client.post(
        f'/v3/{customer_id}/BankingServices/GetStatements',
        data=json.dumps({'LoginId': str(login_id), 'AccountId': 'invalid_id'}),
        content_type='application/json'
    )
    assert response.status_code == 404
    data = json.loads(response.data)
    assert data['error'] == 'Invalid AccountId'
