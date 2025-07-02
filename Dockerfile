# ===== Base Stage =====
# This stage installs the common Python dependencies for both services.
FROM python:3.11-slim as base
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# ===== API Stage =====
# This stage builds the image for the mock API service.
FROM base as api
COPY ./api_mock ./api_mock
COPY ./api_mock/config.json .
COPY run.py .
EXPOSE 5000
CMD ["flask", "run", "--host=0.0.0.0"]


# ===== Underwriter App Stage =====
# This stage builds the image for the Streamlit application.
FROM base as underwriter_app

# force un-buffered, UTF-8 output for all Python processes
ENV PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=UTF-8

COPY ./app ./app
WORKDIR /app
COPY ./api_mock/config.json .
COPY ./pipelines ./pipelines
COPY ./analytics ./analytics
RUN mkdir -p /app/pipelines/data_lake/bronze && \
    mkdir -p /app/pipelines/data_lake/analytics
EXPOSE 8501
CMD ["streamlit", "run", "1_Manual_CSV_Upload.py", "--server.address=0.0.0.0"]


# ===== dbt Docs Stage =====
# This stage generates and serves the dbt documentation website.
FROM base as dbt_docs
WORKDIR /app/analytics

# Serve the generated documentation on port 8081
EXPOSE 8081
CMD ["python", "-m", "http.server", "8081", "--directory", "./target"]


# ===== Jupyter Stage =====
# This stage runs a JupyterLab server for ad-hoc analysis.
FROM base as jupyter
WORKDIR /app

# Expose the Jupyter port
EXPOSE 8888
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''"]
