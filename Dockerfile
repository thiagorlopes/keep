# Use an official lightweight Python image.
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
# This step is done separately to leverage Docker's layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code and config file into the container
COPY ./app ./app
COPY ./data ./data
COPY config.json .
COPY run.py .

# Expose the port the app runs on
EXPOSE 5000

# Command to run the application
CMD ["python", "run.py"]
