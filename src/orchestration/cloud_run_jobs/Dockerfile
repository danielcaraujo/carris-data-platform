# Use a Python base image (choose the version you prefer)
FROM python:3.9-slim

# Install git and other dependencies
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container
WORKDIR /app

# Install dbt (example with dbt-core; change if you use dbt-postgres, dbt-bigquery, etc.)
RUN pip install dbt-bigquery

# Environment variable to disable output buffering (better logging)
ENV PYTHONUNBUFFERED=1

# Copy the script that will run the workflow
COPY entrypoint.sh /entrypoint.sh

# Make the script executable
RUN chmod +x /entrypoint.sh

# Set the entrypoint to the script
ENTRYPOINT ["/entrypoint.sh"]