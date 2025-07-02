# Mock Flinks API Server

This project provides a mock implementation of a subset of the [Flinks API](https://docs.flinks.com/docs/welcome?_gl=1*je0k2v*_gcl_au*NDY3Mzk0Mzc5LjE3NTEwNDk5NzU.), designed for local development and testing. It includes a web interface for easy interaction and is fully containerized with Docker.

## Features

- **Realistic API Endpoints**: Simulates key Flinks endpoints like `/Authorize`, `/GetAccountsDetail`, and `/GetStatements`.
- **JSON-based API**: The primary data endpoint returns a structured JSON payload with base64-encoded statement data, mimicking modern API design.
- **Interactive Web UI**: A clean, user-friendly frontend to query the API by email and download a `.zip` archive of all associated statements.
- **Dockerized Environment**: Includes a `Dockerfile` for easy, one-command setup and execution, eliminating the need for local Python environment management.
- **Clean Architecture**: The application is structured with a clear separation of concerns (API routes, services, data).
- **Comprehensive Test Suite**: Includes unit and integration tests built with `pytest`.
- **High-Fidelity Data**: Uses real data from the provided CSV files to ensure realistic mock responses.

## Project Structure

The project follows a standard Flask application structure:

```
.
├── api_mock/             # Main Flask application
│   ├── api/              # API blueprint and routes
│   ├── services/         # Business logic and data access
│   ├── static/           # Static files (CSS, JS)
│   ├── templates/        # HTML templates
│   └── __init__.py       # Application factory
├── pipelines/            # Data pipelines and local data lake
│   ├── data_lake/        # The data lake (bronze, silver, etc.)
│   └── ...
├── analytics/            # dbt and DuckDB analytics project
├── tests/                # Pytest test suite
├── .dockerignore         # Files to ignore in the Docker build
├── Dockerfile            # Docker configuration
├── requirements.txt      # Python dependencies
└── run.py                # Application entry point
```

## Setup and Installation

### Prerequisites

- Python 3.10+
- Docker Desktop (or Docker Engine)

### 1. Docker Setup (Recommended)

This is the easiest and recommended way to run the application.

**A. Build the Docker image:**
```bash
docker build -t flinks-mock-api .
```

**B. Run the Docker container:**
```bash
docker run -d --rm -p 5000:5000 flinks-mock-api
```
The application will be available at `http://localhost:5000`.

### 3. Development with Hot-Reloading (Docker Compose)

For active development, using Docker Compose is the most efficient method. It automatically reloads the server inside the container whenever you save a file.

**A. Start the service:**
```bash
docker compose up
```

**B. Stop the service:**
When you are finished, press `Ctrl+C` in the terminal, or run:
```bash
docker compose down
```

### 2. Local Python Environment Setup

If you prefer to run the application locally without Docker:

**A. Create and activate a virtual environment:**
```bash
python3 -m venv venv
source venv/bin/activate
```

**B. Install the dependencies:**
```bash
pip install -r requirements.txt
```

**C. Run the application:**
```bash
python run.py
```
The application will be available at [http://localhost:5000](http://localhost:5000).

## API Documentation

### Base URL
```
http://localhost:5000
```

### Authentication
The mock API does not require authentication for development purposes. In production, this would be replaced with proper API key authentication following the Flinks API specification.

### Web Interface Endpoints

#### 1. Main Page
- **Endpoint**: `GET /`
- **Description**: Serves the main web interface for manual statement downloads
- **Response**: HTML page

**Example:**
```bash
curl -X GET http://localhost:5000/
```

#### 2. Download Statement by Email
- **Endpoint**: `POST /download`
- **Description**: Downloads bank statement data for all accounts associated with an email address
- **Content-Type**: `application/x-www-form-urlencoded`
- **Parameters**:
  - `email` (required): Email address of the account holder

**Example:**
```bash
curl -X POST http://localhost:5000/download \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "email=joelschaubel@gmail.com"
```

**Response:**
```json
{
  "Email": "joelschaubel@gmail.com",
  "Statements": [
    {
      "AccountNumber": "010-30800-0095971396",
      "AccountType": "Operation",
      "FileName": "statement_010-30800-0095971396.csv",
      "Content_Base64": "RGF0ZSxEZXNjcmlwdGlvbixXaXRoZHJhd2Fscw=="
    }
  ]
}
```

### Flinks API Mock Endpoints

These endpoints simulate the Flinks Banking API for development and testing.

#### 1. Authorize
- **Endpoint**: `POST /v3/{customerId}/BankingServices/Authorize`
- **Description**: Mock authorization endpoint that generates login credentials
- **Content-Type**: `application/json`
- **Path Parameters**:
  - `customerId` (UUID): Customer identifier
- **Request Body**: JSON object (can be empty for mock)

**Example:**
```bash
curl -X POST http://localhost:5000/v3/550e8400-e29b-41d4-a716-446655440000/BankingServices/Authorize \
  -H "Content-Type: application/json" \
  -d '{}'
```

**Response:**
```json
{
  "LoginId": "123e4567-e89b-12d3-a456-426614174000",
  "RequestId": "987fcdeb-51a2-43d1-b789-123456789abc",
  "StatusCode": 200
}
```

#### 2. Get Accounts Detail
- **Endpoint**: `POST /v3/{customerId}/BankingServices/GetAccountsDetail`
- **Description**: Retrieves detailed information about all accounts for a customer
- **Content-Type**: `application/json`
- **Path Parameters**:
  - `customerId` (UUID): Customer identifier
- **Request Body**:
  - `LoginId` (required): Login ID from the authorize response

**Example:**
```bash
curl -X POST http://localhost:5000/v3/550e8400-e29b-41d4-a716-446655440000/BankingServices/GetAccountsDetail \
  -H "Content-Type: application/json" \
  -d '{
    "LoginId": "123e4567-e89b-12d3-a456-426614174000"
  }'
```

**Response:**
```json
{
  "Accounts": [
    {
      "Id": "001_statement_a",
      "AccountNumber": "010-30800-0095971396",
      "Type": "Operation",
      "Balance": {
        "Current": 2016.70
      },
      "Holder": {
        "Name": "Joel Schaubel",
        "Email": "joelschaubel@gmail.com"
      },
      "Institution": "Simplii"
    }
  ],
  "Login": {
    "Id": "123e4567-e89b-12d3-a456-426614174000"
  },
  "Institution": "Flinks Capital",
  "RequestId": "987fcdeb-51a2-43d1-b789-123456789abc"
}
```

#### 3. Get Statements
- **Endpoint**: `POST /v3/{customerId}/BankingServices/GetStatements`
- **Description**: Retrieves bank statement transactions for a specific account
- **Content-Type**: `application/json`
- **Path Parameters**:
  - `customerId` (UUID): Customer identifier
- **Request Body**:
  - `LoginId` (required): Login ID from the authorize response
  - `AccountNumber` (required): Account number to retrieve statements for

> **Important:** The request body must contain the `AccountNumber`, not the internal `AccountId`. The API uses the `AccountNumber` to look up the account details. Using `AccountId` will result in a `400 Bad Request` error.

**Example:**
```bash
curl -X POST http://localhost:5000/v3/123e4567-e89b-12d3-a456-426614174000/BankingServices/GetStatements \
  -H "Content-Type: application/json" \
  -d '{"LoginId": "abc-123", "AccountNumber": "010-30800-0095971396"}'
```

**Response:**
```json
{
  "Statements": [
    {
      "Id": "456e7890-e12b-34c5-d678-901234567def",
      "AccountId": "001_statement_a",
      "Transactions": [
        {
          "Date": "2024-01-15",
          "Description": "Direct Deposit - Salary",
          "Amount": 3500.00,
          "Type": "credit",
          "Balance": "5516.70"
        },
        {
          "Date": "2024-01-14",
          "Description": "ATM Withdrawal",
          "Amount": -200.00,
          "Type": "debit",
          "Balance": "2016.70"
        }
      ]
    }
  ],
  "Login": {
    "Id": "123e4567-e89b-12d3-a456-426614174000"
  },
  "RequestId": "789abcde-f123-45g6-h789-ijklmnopqrst"
}
```

### Complete API Workflow Example

Here's a complete example of how to use the Flinks API mock endpoints in sequence:

```bash
# 1. Authorize and get LoginId
CUSTOMER_ID="550e8400-e29b-41d4-a716-446655440000"
AUTH_RESPONSE=$(curl -s -X POST http://localhost:5000/v3/$CUSTOMER_ID/BankingServices/Authorize \
  -H "Content-Type: application/json" \
  -d '{}')

LOGIN_ID=$(echo $AUTH_RESPONSE | jq -r '.LoginId')
echo "Login ID: $LOGIN_ID"

# 2. Get account details
ACCOUNTS_RESPONSE=$(curl -s -X POST http://localhost:5000/v3/$CUSTOMER_ID/BankingServices/GetAccountsDetail \
  -H "Content-Type: application/json" \
  -d "{\"LoginId\": \"$LOGIN_ID\"}")

ACCOUNT_NUMBER=$(echo $ACCOUNTS_RESPONSE | jq -r '.Accounts[0].AccountNumber')
echo "Account Number: $ACCOUNT_NUMBER"

# 3. Get statements for the account
curl -X POST http://localhost:5000/v3/$CUSTOMER_ID/BankingServices/GetStatements \
  -H "Content-Type: application/json" \
  -d "{
    \"LoginId\": \"$LOGIN_ID\",
    \"AccountNumber\": \"$ACCOUNT_NUMBER\"
  }" | jq '.'
```

### Error Handling

All endpoints return appropriate HTTP status codes and error messages:

- `400 Bad Request`: Missing required parameters or invalid request format
- `404 Not Found`: Account not found or email not found
- `500 Internal Server Error`: Server-side errors

**Error Response Format:**
```json
{
  "error": "Description of the error"
}
```

### Test Data

The mock API includes test data for the following account:
- **Email**: `joelschaubel@gmail.com`
- **Account Numbers**:
  - `010-30800-0095971396`
  - `010-30800-0095983938`
- **Account Holder**: Joel Schaubel
- **Institution**: Simplii

## Usage

### Web Interface

Navigate to `http://localhost:5000` in your web browser.

- **Email**: `joelschaubel@gmail.com`

Enter the email address into the input field. The application will make a request to the API and generate a `.zip` file containing all statements associated with that user.

## Running the Test Suite

To run the tests, ensure you have set up a local Python environment and installed the dependencies.

Run the full test suite with `pytest`:
```bash
python -m pytest -v
```
