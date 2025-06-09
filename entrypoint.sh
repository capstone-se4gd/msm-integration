#!/bin/bash

# Load environment variables from .env file
if [ -f .env ]; then
    echo "Loading environment variables from .env file..."
    export $(grep -v '^#' .env | xargs)
else
    echo "Error: .env file not found!"
    exit 1
fi

# Check if all required MySQL environment variables are set
if [ -z "$DB_HOST" ] || [ -z "$DB_USER" ] || [ -z "$DB_PASSWORD" ] || [ -z "$DB_NAME" ]; then
    echo "Error: MySQL credentials not properly configured in .env file!"
    echo "Please ensure DB_HOST, DB_USER, DB_PASSWORD, and DB_NAME are set."
    exit 1
fi

# Check MySQL connection and if database exists
echo "Checking MySQL database connection..."
if python -c "
import pymysql
import os
try:
    connection = pymysql.connect(
        host=os.environ['DB_HOST'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        database=os.environ['DB_NAME'],
        port=int(os.environ.get('DB_PORT', 3306))
    )
    connection.close()
    print('Connection successful, database exists')
    exit(0)
except Exception as e:
    print(f'Database connection failed: {e}')
    exit(1)
"; then
    echo "Database already exists, skipping setup."
else
    echo "Database setup needed. Running initial setup..."
    python setup_database.py
    echo "Database setup completed."
fi

# Then run the Flask app
exec flask run --host=0.0.0.0