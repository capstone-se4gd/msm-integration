#!/bin/bash
# Run the DB setup script
python setup_database.py 

# Then run the Flask app
exec flask run