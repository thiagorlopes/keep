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

# --- New Tests for Increased Coverage ---

def test_index_route(client):
    """Test the index route."""
    response = client.get('/')
    assert response.status_code == 200
    assert b"<title>Flinks Mock Interface</title>" in response.data

def test_get_accounts_detail_missing_loginid(client):
    """Test GetAccountsDetail endpoint when LoginId is missing."""
    response = client.post(f'/v3/{uuid4()}/BankingServices/GetAccountsDetail', data=json.dumps({}), content_type='application/json')
    assert response.status_code == 400
    data = json.loads(response.data)
    assert data['error'] == 'LoginId is required'

def test_get_statements_missing_payload(client):
    """Test GetStatements endpoint with missing payload keys."""
    # Missing LoginId
    response = client.post(
        f'/v3/{uuid4()}/BankingServices/GetStatements',
        data=json.dumps({'AccountNumber': '123'}),
        content_type='application/json'
    )
    assert response.status_code == 400
    assert json.loads(response.data)['error'] == 'LoginId is required'
    
    # Missing AccountNumber
    response = client.post(
        f'/v3/{uuid4()}/BankingServices/GetStatements',
        data=json.dumps({'LoginId': str(uuid4())}),
        content_type='application/json'
    )
    assert response.status_code == 400
    assert json.loads(response.data)['error'] == 'AccountNumber is required'

def test_download_statement_success(client):
    """Test the download_statement endpoint for a successful case."""
    response = client.post('/download', data={'email': 'joelschaubel@gmail.com'})
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data['Email'] == 'joelschaubel@gmail.com'
    assert len(data['Statements']) == 2
    assert 'Content_Base64' in data['Statements'][0]

def test_download_statement_email_not_found(client):
    """Test the download_statement endpoint for an email that doesn't exist."""
    response = client.post('/download', data={'email': 'nonexistent@email.com'})
    assert response.status_code == 404
    data = json.loads(response.data)
    assert 'not found' in data['error']

def test_download_statement_missing_email(client):
    """Test the download_statement endpoint when email is missing."""
    response = client.post('/download')
    assert response.status_code == 400
    data = json.loads(response.data)
    assert 'Email is required' in data['error']

def test_get_combined_statements_by_email_success(client):
    """Test get_combined_statements_by_email for a successful case."""
    response = client.get('/api/statements?email=joelschaubel@gmail.com')
    assert response.status_code == 200
    data = json.loads(response.data)
    # Based on mock data files, the user has transactions in both files
    assert len(data) > 2

def test_get_combined_statements_email_not_found(client):
    """Test get_combined_statements_by_email for a non-existent email."""
    response = client.get('/api/statements?email=nonexistent@email.com')
    assert response.status_code == 404
    data = json.loads(response.data)
    assert 'not found' in data['error']

def test_get_combined_statements_missing_email(client):
    """Test get_combined_statements_by_email when the email parameter is missing."""
    response = client.get('/api/statements')
    assert response.status_code == 400
    data = json.loads(response.data)
    assert 'Email query parameter is required' in data['error']
