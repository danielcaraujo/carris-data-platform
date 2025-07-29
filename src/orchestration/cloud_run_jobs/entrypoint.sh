#!/bin/bash
set -e

# Git repos url
REPO_URL=${REPO_URL:-"https://github.com/danielcaraujo/carris-data-platform.git"}

# Folder where is dbt code
APP_DIR="./carris-data-platform/src/marts/dbt/carris_transformations"

# Check if projectID env var is defined
if [ -z "$PROJECT_ID" ]; then
  echo "Error: PROJECT_ID env var is not defined."
  exit 1
fi

echo "Using PROJECT_ID: $PROJECT_ID"

# If the directory exists, pull the latest changes; otherwise, clone the repository
if [ -d "$APP_DIR" ]; then
  echo "Updating existing repository..."
  cd $APP_DIR
  git pull
else
  echo "Cloning repository..."
  git clone $REPO_URL
  cd $APP_DIR
fi

dbt deps

dbt build --vars "{PROJECT_ID: '$PROJECT_ID'}"
