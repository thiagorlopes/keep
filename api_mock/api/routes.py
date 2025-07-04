import os
import io
import csv
import zipfile
import base64
from uuid import uuid4
from flask import Blueprint, jsonify, request, render_template, flash, send_from_directory, redirect, url_for, Response, current_app
from api_mock.services import data_service
import logging

logging.basicConfig(level=logging.INFO)

api_bp = Blueprint('api', __name__, static_folder='static')

@api_bp.route('/', methods=['GET'])
def index():
    """Serves the main page."""
    current_app.logger.info("Serving index page.")
    return render_template('index.html')

@api_bp.route('/download', methods=['POST'])
def download_statement():
    """
    Handles statement download requests from the main page by email.
    Returns a JSON response with base64-encoded CSV data for each account.
    """
    email = request.form.get('email')
    if not email:
        current_app.logger.error("Download request received without an email address.")
        return jsonify({'error': 'Email is required.'}), 400

    current_app.logger.info(f"Download request received for email: {email}")
    account_ids = data_service.get_account_ids_by_email(email)

    if not account_ids:
        # This is an API-like endpoint now, so we return JSON for errors too.
        current_app.logger.warning(f"No accounts found for email: {email}")
        return jsonify({'error': f"Email '{email}' not found."}), 404

    statements_data = []
    current_app.logger.info(f"Found {len(account_ids)} account(s) for email: {email}")
    for account_id in account_ids:
        account_details = next((acc for acc in data_service.ACCOUNTS if acc['Id'] == account_id), {})
        transactions, headers = data_service.load_raw_transactions(account_id)

        if not transactions:
            current_app.logger.info(f"No transactions found for account ID: {account_id}. Skipping.")
            continue

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=headers)
        writer.writeheader()
        writer.writerows(transactions)

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
        current_app.logger.warning(f"No transactions found across all accounts for email: {email}")
        return jsonify({'error': f"No transactions found for email '{email}'."}), 404

    current_app.logger.info(f"Successfully prepared {len(statements_data)} statement(s) for email: {email}")
    return jsonify({
        'Email': email,
        'Statements': statements_data
    })

@api_bp.route('/api/statements', methods=['GET'])
def get_combined_statements_by_email():
    """
    A simple endpoint to get all transactions for a given email,
    combined from multiple statements into a single list.
    """
    email = request.args.get('email')
    if not email:
        return jsonify({'error': 'Email query parameter is required.'}), 400

    current_app.logger.info(f"Combined statement request received for email: {email}")
    account_ids = data_service.get_account_ids_by_email(email)

    if not account_ids:
        return jsonify({'error': f"Email '{email}' not found."}), 404

    all_transactions = []
    # Use a set to keep track of seen transaction identifiers to avoid duplicates
    # assuming a tuple of key fields can uniquely identify a transaction
    seen_transactions = set()

    for account_id in account_ids:
        transactions, _ = data_service.load_raw_transactions(account_id)
        for t in transactions:
            # Create a unique identifier for the transaction to avoid duplicates
            # This assumes that (Date, Description, Amount) is unique. Adjust if needed.
            transaction_id = (t.get('Date'), t.get('Description'), t.get('Deposits'), t.get('Withdrawals'))
            if transaction_id not in seen_transactions:
                all_transactions.append(t)
                seen_transactions.add(transaction_id)

    if not all_transactions:
        return jsonify({'error': f"No transactions found for email '{email}'."}), 404

    current_app.logger.info(f"Successfully combined {len(all_transactions)} transactions for email: {email}")
    return jsonify(all_transactions)

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

    account_number = data.get('AccountNumber')
    if not account_number:
        return jsonify({'error': 'AccountNumber is required'}), 400

    # Look up the internal account ID from the account number
    account_id = data_service.get_account_id_by_number(account_number)
    if not account_id:
        return jsonify({'error': f"Account with AccountNumber '{account_number}' not found."}), 404

    # The rest of the logic uses the internal account_id
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
