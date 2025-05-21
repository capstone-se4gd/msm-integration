#!/bin/bash

# Only run database setup if database.db doesn't exist
if [ ! -f database.db ]; then
    echo "Database not found. Running initial setup..."
    python setup_database.py
    echo "Database setup completed."
else
    echo "Database already exists, skipping setup."
fi

# Then run the Flask app
exec flask run