#!/bin/bash
set -e

# Git repos url
REPO_URL=${REPO_URL:-"https://github.com/danielcaraujo/carris-data-platform/tree/main"}

# Folder where is dbt code
APP_DIR="/src/marts/dbt/carris_transformations"

# Se a pasta existe, faz pull, senão clona
if [ -d "$APP_DIR" ]; then
  echo "Repositório já existe, fazendo git pull..."
  cd $APP_DIR
  git pull
else
  echo "Clonando repositório..."
  git clone $REPO_URL $APP_DIR
  cd $APP_DIR
fi

dbt deps

dbt run