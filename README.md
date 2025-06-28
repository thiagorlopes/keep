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
├── app/                  # Main Flask application
│   ├── api/              # API blueprint and routes
│   ├── services/         # Business logic and data access
│   ├── static/           # Static files (CSS, JS)
│   ├── templates/        # HTML templates
│   └── __init__.py       # Application factory
├── data/                 # Raw CSV statement files
├── ingestion_pipeline/   # (Placeholder) For data ingestion scripts
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
The application will be available at `http://127.0.0.1:5000`.

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
The application will be available at `http://127.0.0.1:5000`.


## Usage

### Web Interface

Navigate to `http://127.0.0.1:5000` in your web browser.

- **Email**: `joelschaubel@gmail.com`

Enter the email address into the input field. The application will make a request to the API and generate a `.zip` file containing all statements associated with that user.

### API Endpoint for Data Retrieval

While the UI is convenient, you can also interact directly with the JSON API endpoint. This is the endpoint that a real client application or an ingestion pipeline would use.

**Endpoint**: `POST /download`

This endpoint expects a form-data payload with an `email` key.

**Example `curl` command:**
```bash
curl -X POST -F "email=joelschaubel@gmail.com" http://127.0.0.1:5000/download
```

**Example Successful JSON Response:**
```json
{
  "Email": "joelschaubel@gmail.com",
  "Statements": [
    {
      "AccountNumber": "010-30800-0095971396",
      "AccountType": "Operation",
      "Content_Base64": "VXNlcm5hbWUsRW1haWwsQWRkcmVzcy...",
      "FileName": "statement_010-30800-0095971396.csv"
    },
    {
      "AccountNumber": "010-30800-0095983938",
      "AccountType": "Operation",
      "Content_Base64": "VXNlcm5hbWUsRW1haWwsQWRkcmVzcy...",
      "FileName": "statement_010-30800-0095983938.csv"
    }
  ]
}
```

**Example Error JSON Response:**
```json
{
  "error": "Email 'test@test.com' not found."
}
```

## Running the Test Suite

To run the tests, ensure you have set up a local Python environment and installed the dependencies.

Run the full test suite with `pytest`:
```bash
python -m pytest -v
```
