#!/bin/bash
set -euo pipefail

echo "Starting PostgreSQL..."
docker-compose up -d db

echo "Waiting for database..."
sleep 3

echo "Loading data..."
docker-compose run --rm app python load_data.py \
    --host db --dbname transit --user transit --password transit123 --datadir /app/data

echo ""
echo "Running sample queries..."
docker-compose run --rm app python queries.py --query Q1 --host db --dbname transit --user transit --password transit123
docker-compose run --rm app python queries.py --query Q3 --host db --dbname transit --user transit --password transit123
