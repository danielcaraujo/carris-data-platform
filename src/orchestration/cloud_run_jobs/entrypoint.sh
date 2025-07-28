#!/bin/bash
set -e

# Git repos url
REPO_URL=${REPO_URL:-"https://github.com/danielcaraujo/carris-data-platform.git"}

# Folder where is dbt code
APP_DIR="/src/marts/dbt/carris_transformations"

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

dbt build
