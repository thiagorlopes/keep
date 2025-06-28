import os
import io
import csv
import zipfile
import base64
from uuid import uuid4
from flask import Blueprint, jsonify, request, render_template, flash, send_from_directory, redirect, url_for, Response
from app.services import data_service

api_bp = Blueprint('api', __name__, static_folder='static')

@api_bp.route('/', methods=['GET'])
def index():
    """Serves the main page."""
    return render_template('index.html')

@api_bp.route('/download', methods=['POST'])
def download_statement():
    """
    Handles statement download requests from the main page by email.
    Returns a JSON response with base64-encoded CSV data for each account.
    """
    email = request.form.get('email')
    account_ids = data_service.get_account_ids_by_email(email)

    if not account_ids:
        # This is an API-like endpoint now, so we return JSON for errors too.
        return jsonify({'error': f"Email '{email}' not found."}), 404

    statements_data = []
    for account_id in account_ids:
        account_details = next((acc for acc in data_service.ACCOUNTS if acc['Id'] == account_id), {})
        transactions = data_service.load_transactions(account_id)

        if not transactions:
            continue

        output = io.StringIO()
        headers = ['AccountNumber', 'AccountType', 'Date', 'Description', 'Amount', 'Type', 'Balance']
        writer = csv.DictWriter(output, fieldnames=headers)
        writer.writeheader()

        for t in transactions:
            t['AccountNumber'] = account_details.get('AccountNumber')
            t['AccountType'] = account_details.get('Type')
            writer.writerow(t)

        # Encode the CSV data as a base64 string
        csv_string = output.getvalue()
        csv_base64 = base64.b64encode(csv_string.encode('utf-8')).decode('utf-8')

        statements_data.append({
            'AccountNumber': account_details.get('AccountNumber'),
            'AccountType': account_details.get('Type'),
            'FileName': f"statement_{account_details.get('AccountNumber')}.csv",
            'Content_Base64': csv_base64
        })

    if not statements_data:
        return jsonify({'error': f"No transactions found for email '{email}'."}), 404

    return jsonify({
        'Email': email,
        'Statements': statements_data
    })

@api_bp.route('/v3/<uuid:customerId>/BankingServices/Authorize', methods=['POST'])
def authorize(customerId):
    """Mock Authorize endpoint."""
    return jsonify({
        'LoginId': str(uuid4()),
        'RequestId': str(uuid4()),
        'StatusCode': 200,
    })

@api_bp.route('/v3/<uuid:customerId>/BankingServices/GetAccountsDetail', methods=['POST'])
def get_accounts_detail(customerId):
    """Mock GetAccountsDetail endpoint."""
    data = request.get_json()
    if not data or 'LoginId' not in data:
        return jsonify({'error': 'LoginId is required'}), 400

    accounts = data_service.get_accounts()
    return jsonify({
        'Accounts': accounts,
        'Login': {'Id': data['LoginId']},
        'Institution': 'Flinks Capital',
        'RequestId': str(uuid4()),
    })

@api_bp.route('/v3/<uuid:customerId>/BankingServices/GetStatements', methods=['POST'])
def get_statements(customerId):
    """Mock GetStatements endpoint."""
    data = request.get_json()
    if not data or 'LoginId' not in data:
        return jsonify({'error': 'LoginId is required'}), 400

    account_id = data.get('AccountId')
    if not account_id:
        return jsonify({'error': 'AccountId is required'}), 400

    if not data_service.is_valid_account(account_id):
        return jsonify({'error': 'Invalid AccountId'}), 404

    transactions = data_service.load_transactions(account_id)

    return jsonify({
        'Statements': [
            {
                'Id': str(uuid4()),
                'AccountId': account_id,
                'Transactions': transactions,
            }
        ],
        'Login': {'Id': data['LoginId']},
        'RequestId': str(uuid4()),
    })
