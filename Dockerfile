# Use an official lightweight Python image.
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
# This step is done separately to leverage Docker's layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code and config file into the container
COPY ./api_mock ./api_mock
COPY config.json .

# Expose the port the app runs on
EXPOSE 5000

# Command to run the application
ENV FLASK_APP=run.py
CMD ["flask", "run", "--host=0.0.0.0"]
