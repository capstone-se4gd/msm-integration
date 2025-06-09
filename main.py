from flask import Flask, request
import sqlite3
import json
from datetime import datetime, timedelta
import aiohttp
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from flask_restx import Api, Resource, fields, Namespace
from routes.invoices import invoice_ns
from routes.batches import batch_ns
from routes.products import product_ns
from routes.emissions import emissions_ns
from models import register_models
from auth import register_auth_routes
import time

app = Flask(__name__)

# Initialize Flask-RESTx
authorizations = {
    'apikey': {
        'type': 'apiKey',
        'in': 'header',
        'name': 'Authorization',
        'description': "Type in the *'Value'* input box below: **'&lt;JWT&gt;'**, where JWT is the token obtained from the login endpoint."
    }
}

api = Api(
    app, 
    version='1.0', 
    title='Sustainability Metrics API',
    description='API for processing invoices and calculating sustainability metrics',
    doc='/api/docs',
    authorizations=authorizations,
    security='apikey'
)

models = register_models(api)
invoice_ns.models = models
batch_ns.models = models
product_ns.models = models
emissions_ns.models = models
auth_ns = Namespace('auth', description='Authentication operations')

# Add namespaces to the API
api.add_namespace(auth_ns, path='/api/auth')
api.add_namespace(invoice_ns, path='/api')
api.add_namespace(batch_ns, path='/api')
api.add_namespace(product_ns, path='/api')
api.add_namespace(emissions_ns, path='/api')

app = register_auth_routes(app, auth_ns)

@app.before_request
def start_timer():
    request._start_time = time.perf_counter()

@app.after_request
def log_request_performance(response):
    if hasattr(request, '_start_time'):
        duration = time.perf_counter() - request._start_time
        print(f"[PERF] {request.method} {request.path} took {duration:.4f} seconds")
        response.headers['X-Request-Duration'] = f"{duration:.4f}s"
    return response

# Custom error handler for the API

@api.errorhandler(Exception)
def handle_exception(error):
    """Global error handler for the API"""
    # Log the error for debugging
    print(f"API Error: {str(error)}")
    
    # Determine what kind of exception it is and return appropriate response
    if isinstance(error, sqlite3.Error):
        return {'error': 'Database error occurred'}, 500
    elif isinstance(error, aiohttp.ClientError):
        return {'error': 'Error communicating with external service'}, 503
    elif isinstance(error, json.JSONDecodeError):
        return {'error': 'Error processing JSON data'}, 400
    else:
        # Generic error
        return {'error': f'An unexpected error occurred: {str(error)}'}, 500
# Main function to run the Flask app
if __name__ == '__main__':
    app.run(debug=True, port=5000, host='0.0.0.0')