from flask import Flask, request, jsonify, make_response
import os
import sqlite3
import uuid
import json
from datetime import datetime, timedelta
import asyncio
import aiohttp
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
import threading
from functools import wraps
from flask_restx import Api, Resource, fields, Namespace
import requests

# Import the authentication module
from auth import register_auth_routes, token_required, query_db, execute_db

app = Flask(__name__)

# Initialize Flask-RESTx
authorizations = {
    'apikey': {
        'type': 'apiKey',
        'in': 'header',
        'name': 'Authorization',
        'description': "Type in the *'Value'* input box below: **'Bearer &lt;JWT&gt;'**, where JWT is the token"
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

# Create namespaces
auth_ns = Namespace('auth', description='Authentication operations')
invoice_ns = Namespace('invoices', description='Invoice processing operations')
batch_ns = Namespace('batches', description='Batch management operations')
product_ns = Namespace('products', description='Product management operations')
emissions_ns = Namespace('emissions', description='Emissions data operations')

# Add namespaces to the API
api.add_namespace(auth_ns, path='/api/auth')
api.add_namespace(invoice_ns, path='/api')
api.add_namespace(batch_ns, path='/api')
api.add_namespace(product_ns, path='/api')
api.add_namespace(emissions_ns, path='/api')

# Database file path
DATABASE = 'database.db'

# Register authentication routes (this will be modified to use flask-restx)
app = register_auth_routes(app)

# Define models for request and response
# Transaction processing models
transaction_response = api.model('TransactionResponse', {
    'message': fields.String(description='Status message'),
    'transaction_id': fields.String(description='Unique transaction ID')
})

transaction_detail = api.model('TransactionDetail', {
    'id': fields.String(description='Transaction ID'),
    'result': fields.Raw(description='Processing result'),
    'created_at': fields.String(description='Creation timestamp'),
    'deletion_scheduled_at': fields.String(description='Scheduled deletion time')
})

# Batch creation models
invoice_metrics_model = api.model('InvoiceMetrics', {
    'name': fields.String(description='Metric name'),
    'value': fields.Float(description='Metric value')
})

invoice_model = api.model('Invoice', {
    'facility': fields.String(description='Facility name'),
    'organizationalUnit': fields.String(description='Organizational unit'),
    'url': fields.String(description='Supplier URL'),
    'subCategory': fields.String(description='Sub-category'),
    'invoiceNumber': fields.String(description='Invoice number'),
    'invoiceDate': fields.String(description='Invoice date'),
    'emissionsArePerUnit': fields.String(description='Whether emissions are per unit'),
    'quantityNeededPerUnit': fields.Float(description='Quantity needed per unit'),
    'unitsBought': fields.Float(description='Units bought'),
    'totalAmount': fields.Float(description='Total amount'),
    'currency': fields.String(description='Currency'),
    'transactionStartDate': fields.String(description='Transaction start date'),
    'transactionEndDate': fields.String(description='Transaction end date'),
    'sustainabilityMetrics': fields.List(fields.Nested(invoice_metrics_model), description='Sustainability metrics'),
    'productName': fields.String(description='Product name')
})

batch_request = api.model('BatchRequest', {
    'productName': fields.String(required=True, description='Product name'),
    'productId': fields.String(description='Product ID (optional, new one will be created if not provided)'),
    'invoices': fields.List(fields.Nested(invoice_model), required=True, description='List of invoices')
})

batch_response = api.model('BatchResponse', {
    'message': fields.String(description='Status message'),
    'productId': fields.String(description='Product ID'),
    'batchId': fields.String(description='Batch ID')
})

# Product models
batch_info = api.model('BatchInfo', {
    'id': fields.String(description='Batch ID'),
    'created_at': fields.String(description='Creation timestamp'),
    'carbon_footprint': fields.Float(description='Carbon footprint'),
    'water_usage': fields.Float(description='Water usage'),
    'energy_consumption': fields.Float(description='Energy consumption')
})

product_detail = api.model('ProductDetail', {
    'productId': fields.String(description='Product ID'),
    'productName': fields.String(description='Product name'),
    'batches': fields.List(fields.Nested(batch_info), description='List of batches')
})

sustainability_metric = api.model('SustainabilityMetric', {
    'metric_id': fields.String(description='Metric ID'),
    'name': fields.String(description='Metric name'),
    'value': fields.Float(description='Metric value'),
    'unit': fields.String(description='Metric unit')
})

product_list_item = api.model('ProductListItem', {
    'productId': fields.String(description='Product ID'),
    'productName': fields.String(description='Product name'),
    'sustainabilityMetrics': fields.List(fields.Nested(sustainability_metric), description='Sustainability metrics')
})

# Emissions model
emissions_model = api.model('EmissionsData', {
    'product_id': fields.String(description='Product ID'),
    'name': fields.String(description='Name'),
    'product': fields.String(description='Product'),
    'company': fields.String(description='Company'),
    'source': fields.String(description='Source'),
    'category': fields.String(description='Category'),
    'sub_category': fields.String(description='Sub-category'),
    'CO2E': fields.Float(description='CO2 equivalent'),
    'CO2E_unit': fields.String(description='CO2 equivalent unit'),
    'quantity': fields.Float(description='Quantity'),
    'quantity_unit': fields.String(description='Quantity unit'),
    'timestamp': fields.String(description='Timestamp'),
    'consumption_end_date': fields.String(description='Consumption end date'),
    'emission_factor': fields.Float(description='Emission factor'),
    'emission_factor_library': fields.String(description='Emission factor library'),
    'transaction_start_date': fields.String(description='Transaction start date'),
    'transaction_end_date': fields.String(description='Transaction end date'),
    'water_transaction_type': fields.String(description='Water transaction type'),
    'organizational_unit': fields.String(description='Organizational unit'),
    'facility': fields.String(description='Facility')
})

# Schedule transaction deletion
def schedule_transaction_deletion(transaction_id, hours=24):
    """Schedule transaction to be deleted after specified hours."""
    deletion_time = datetime.utcnow() + timedelta(hours=hours)
    
    # Update transaction with scheduled deletion time
    execute_db(
        'UPDATE transactions SET deletion_scheduled_at = ? WHERE id = ?',
        [deletion_time.isoformat(), transaction_id]
    )
    
    # Create a timer to delete the transaction
    def delete_transaction():
        with app.app_context():
            try:
                conn = sqlite3.connect(DATABASE)
                cursor = conn.cursor()
                cursor.execute('DELETE FROM transactions WHERE id = ?', [transaction_id])
                conn.commit()
                conn.close()
                print(f"Transaction {transaction_id} deleted as scheduled")
            except Exception as e:
                print(f"Error deleting transaction {transaction_id}: {e}")
    
    # Schedule the deletion task
    timer = threading.Timer(hours * 3600, delete_transaction)
    timer.daemon = True
    timer.start()

# Helper function to parse XML and convert to JSON
def xml_to_json(xml_content):
    """Convert Finvoice XML content to JSON."""
    try:
        # Define the Finvoice namespace
        namespaces = {'fin': 'http://www.finvoice.fi/Finvoice'}

        # Parse the XML content
        root = ET.fromstring(xml_content)

        # Helper function to strip namespace
        def strip_namespace(tag):
            return tag.split('}', 1)[1] if '}' in tag else tag

        # Recursive function to process elements
        def process_element(element):
            data = {}
            for child in element:
                tag = strip_namespace(child.tag)
                if list(child):
                    data[tag] = process_element(child)
                else:
                    data[tag] = child.text.strip() if child.text else ''
            return data

        result = {}
        for child in root:
            tag = strip_namespace(child.tag)
            if tag == 'InvoiceRow':
                # Process all InvoiceRow elements
                invoice_rows = []
                for invoice_row in root.findall('fin:InvoiceRow', namespaces):
                    row_data = {}
                    for elem in invoice_row:
                        elem_tag = strip_namespace(elem.tag)
                        if elem_tag == 'SpecificationDetails':
                            # Extract SpecificationFreeText
                            spec_texts = [spec.text.strip() for spec in elem.findall('fin:SpecificationFreeText', namespaces) if spec.text]
                            row_data['SpecificationDetails'] = spec_texts
                        elif elem_tag == 'Other':
                            result['other_url'] = elem.text.strip() if elem.text else ''
                            row_data['Other'] = elem.text.strip() if elem.text else ''
                        else:
                            row_data[elem_tag] = elem.text.strip() if elem.text else ''
                    invoice_rows.append(row_data)
                result['InvoiceRows'] = invoice_rows
            else:
                result[tag] = process_element(child)

        return result
    except Exception as e:
        return {'error': f'XML parsing error: {str(e)}'}

# Helper function to fetch data from URL
async def fetch_url_data(url):
    """Fetch data from URL asynchronously."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    return {'error': f'HTTP error: {response.status}'}
    except aiohttp.ClientError as e:
        print(f"ClientError while fetching URL {url}: {str(e)}")
        return {'error': f'Connection error: {str(e)}'}
    except json.JSONDecodeError as e:
        print(f"JSONDecodeError while parsing response from {url}: {str(e)}")
        return {'error': f'Invalid JSON response: {str(e)}'}
    except Exception as e:
        print(f"Unexpected error while fetching {url}: {str(e)}")
        return {'error': f'Fetch error: {str(e)}'}

# Process single XML file
async def process_xml_file(xml_content):
    """Process a single XML file."""
    try:
        # Convert XML to JSON
        json_data = xml_to_json(xml_content)
        
        # If there's an 'other_url', fetch data from it
        if 'other_url' in json_data and 'error' not in json_data:
            sustainability_data = await fetch_url_data(json_data['other_url'])
            
            # Add sustainability metrics to JSON data
            if 'error' not in sustainability_data:
                # First extract sustainability_metrics from the response if available
                if 'sustainability_metrics' in sustainability_data:
                    json_data['sustainabilityMetrics'] = sustainability_data['sustainability_metrics']
                else:
                    # If no sustainability metrics, add error
                    json_data['sustainabilityMetrics'] = {'error': 'No sustainability metrics found'}
            else:
                json_data['sustainabilityMetrics'] = {'error': sustainability_data['error']}
        
        return json_data
    except Exception as e:
        return {'error': f'Processing error: {str(e)}'}

# Process multiple XML files
async def process_multiple_xml_files(xml_files):
    """Process multiple XML files concurrently."""
    tasks = []
    for xml_content in xml_files:
        tasks.append(process_xml_file(xml_content))
    
    return await asyncio.gather(*tasks)

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

# Endpoints with Flask-RESTx
@invoice_ns.route('/process-invoices')
class ProcessInvoices(Resource):
    @invoice_ns.doc('process_invoices')
    @invoice_ns.response(202, 'Processing started', transaction_response)
    @invoice_ns.response(400, 'Bad request')
    @invoice_ns.response(401, 'Unauthorized')
    @token_required
    def post(self, current_user):
        """Upload and process invoice XML files"""    
        # Check if files were uploaded
        if 'files' not in request.files:
            return {'error': 'No files provided'}, 400
        
        files = request.files.getlist('files')
        if not files:
            return {'error': 'No files provided'}, 400
        
        # Read all XML files
        xml_contents = []
        for file in files:
            print(f"Processing file: {file.filename}")
            if file.filename.endswith('.xml'):
                xml_contents.append(file.read().decode('utf-8'))
        
        if not xml_contents:
            return {'error': 'No XML files provided'}, 400
        
        # Generate transaction ID
        transaction_id = str(uuid.uuid4())
        insert = 'INSERT INTO transactions (id, result, created_at) VALUES (?, ?, ?)'
        
        # Process XML files asynchronously
        def process_files():
            update = 'UPDATE transactions SET result = ?, deletion_scheduled_at = ? WHERE id = ?'
            try:
                # Create event loop
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                # Process files
                results = loop.run_until_complete(process_multiple_xml_files(xml_contents))
                
                # Close the loop
                loop.close()

                print(f"Transaction {transaction_id} completed")

                with app.app_context():  # Properly enter the application context
                    try:
                        # Check if transaction ID already exists
                        existing_transaction = query_db('SELECT * FROM transactions WHERE id = ?', [transaction_id], one=True)
                        if not existing_transaction:
                            # Insert new transaction
                            execute_db(
                                insert,
                                [transaction_id, json.dumps(results), datetime.utcnow().isoformat()]
                            )
                        # Save results to database and set deletion time to 24 hours later
                        execute_db(
                            update,
                            [json.dumps(results), (datetime.utcnow() + timedelta(hours=24)).isoformat(), transaction_id]
                        )
                    except Exception as e:
                        print(f"Error saving transaction {transaction_id}: {e}")
                        execute_db(
                            update,
                            [f'Error saving transaction: {str(e)}', (datetime.utcnow() + timedelta(hours=24)).isoformat(), transaction_id]
                        )
                        
                return True

            except Exception as e:
                print(f"Error processing transaction {transaction_id}: {e}")
                with app.app_context():  # Context needed here too
                    execute_db(
                        update,
                        [transaction_id, f'Error processing files: {str(e)}', datetime.utcnow().isoformat()]
                    )
                return False

        
        # Use a thread pool to process files asynchronously
        with ThreadPoolExecutor() as executor:
            executor.submit(process_files)

        # Check if transaction ID already exists
        existing_transaction = query_db('SELECT * FROM transactions WHERE id = ?', [transaction_id], one=True)
        if not existing_transaction:
            execute_db(
                insert,
                [transaction_id, 'Processing started', datetime.utcnow().isoformat()]
            )
        
        return {
            'message': 'Processing started',
            'transaction_id': transaction_id
        }, 202

@invoice_ns.route('/transaction/<transaction_id>')
@invoice_ns.param('transaction_id', 'The transaction identifier')
class Transaction(Resource):
    @invoice_ns.doc('get_transaction')
    @invoice_ns.response(200, 'Success', transaction_detail)
    @invoice_ns.response(404, 'Transaction not found')
    @invoice_ns.response(401, 'Unauthorized')
    @token_required
    def get(self, transaction_id, current_user):
        """Get the result of a transaction by ID"""
        # Get transaction from database
        transaction = query_db('SELECT * FROM transactions WHERE id = ?', [transaction_id], one=True)
        
        if not transaction:
            return {'error': 'Transaction not found'}, 404
        
        # Schedule deletion
        schedule_transaction_deletion(transaction_id)
        
        # Parse result
        try:
            if transaction['result'].startswith('Error'):
                result = {'error': transaction['result']}
            elif transaction['result'] == 'Processing started':
                result = {'status': 'Processing started'}
            else:
                result = json.loads(transaction['result'])
        except Exception as e:
            result = {'error': f'Error parsing result: {str(e)}'}
        
        return {
            'id': transaction_id,
            'result': result,
            'created_at': transaction['created_at'],
            'deletion_scheduled_at': transaction['deletion_scheduled_at']
        }, 200

@batch_ns.route('/create-batch')
class CreateBatch(Resource):
    @batch_ns.doc('create_batch')
    @batch_ns.expect(batch_request)
    @batch_ns.response(200, 'Success', batch_response)
    @batch_ns.response(400, 'Bad request')
    @batch_ns.response(404, 'Product not found')
    @batch_ns.response(500, 'Internal server error')
    @batch_ns.response(401, 'Unauthorized')
    @token_required
    def post(self, current_user):
        """Create a new batch for a product"""
        data = request.get_json()
        
        if not data:
            return {'error': 'No data provided'}, 400
        
        required_fields = ['productName', 'invoices']
        for field in required_fields:
            if field not in data:
                return {'error': f'Missing required field: {field}'}, 400
        
        product_id = data.get('productId')
        product_name = data['productName']
        
        if product_id:
            product = query_db('SELECT * FROM products WHERE id = ?', [product_id], one=True)
            if not product:
                return {'error': 'Product not found'}, 404
        else:
            product_id = str(uuid.uuid4())
            execute_db(
                'INSERT INTO products (id, name, created_at) VALUES (?, ?, ?)',
                [product_id, product_name, datetime.utcnow().isoformat()]
            )
        
        batch_id = str(uuid.uuid4())

        invoices = data['invoices']
        sustainability_metrics = {}
        for invoice in invoices:
            if 'sustainabilityMetrics' in invoice:
                metrics = invoice['sustainabilityMetrics']
                for metric in metrics:
                    if 'name' in metric and 'value' in metric:
                        name = metric['name']
                        value = metric['value']
                        if name not in sustainability_metrics:
                            sustainability_metrics[name] = 0
                        if invoice["emissionsArePerUnit"] == 'YES':
                            sustainability_metrics[name] += value * float(invoice['quantityNeededPerUnit'])
                        else:
                            sustainability_metrics[name] += value / float(invoice['unitsBought']) * float(invoice['quantityNeededPerUnit'])

        try:
            # 1. Fetch defined sustainability metrics
            metrics_url = f"{os.environ.get('LEDGER_URL')}/api/sustainability-metrics/"
            metrics_response = requests.get(metrics_url)
            if metrics_response.status_code != 200:
                raise Exception(f"Failed to fetch sustainability metrics: {metrics_response.status_code}")
            sustainability_metrics_defined = metrics_response.json()

            # 2. Build sustainability metrics input
            sustainability_metrics_input = []
            for metric in sustainability_metrics_defined:
                metric_id = metric['metric_id']
                name = metric['name']
                if name in sustainability_metrics:
                    sustainability_metrics_input.append({
                        'metric_id': metric_id,
                        'value': sustainability_metrics[name]
                    })

            # 3. Format suppliers
            suppliers = []
            for supplier in invoices:
                formatted_metrics = []
                if 'sustainabilityMetrics' in supplier:
                    for metric in supplier['sustainabilityMetrics']:
                        name = metric.get('name')
                        value = metric.get('value')
                        if name and value is not None:
                            metric_id = next((m['metric_id'] for m in sustainability_metrics_defined if m['name'] == name), None)
                            if metric_id:
                                formatted_metrics.append({'metric_id': metric_id, 'value': value})
                    supplier['formattedMetrics'] = formatted_metrics

                # 4. Ensure supplier URL exists or create supplier product
                slug = None
                if supplier.get('url') and supplier['url'] not in ('', 'None'):
                    slug = supplier['url'].split('/')[-1]
                else:
                    new_product = {
                        "name": supplier['productName'],
                        "manufacturer": {
                            "name": "",
                            "mainURL": "http://localhost"
                        },
                        "sustainability_metrics_input": formatted_metrics,
                        "number_of_units": supplier['unitsBought'],
                        "subparts": []
                    }
                    create_url = f"{os.environ.get('LEDGER_URL')}/api/products/"
                    create_response = requests.post(create_url, json=new_product)
                    if create_response.status_code not in (200, 201):
                        raise Exception(f"Failed to create product: {create_response.status_code} - {create_response.text}")
                    created = create_response.json()
                    slug = created.get('slug')
                    if not slug:
                        raise Exception('Product created but no slug returned')
                    supplier['url'] = f"{os.environ.get('LEDGER_URL')}/api/products/{slug}/"

                suppliers.append({
                    "name": supplier['productName'],
                    "sustainability_metrics_input": formatted_metrics,
                    "quantity_needed_per_unit": float(supplier['quantityNeededPerUnit']),
                    "units_bought": float(supplier['unitsBought']),
                    "manufacturer": {
                        "name": "",
                        "mainURL": "http://localhost"
                    },
                    "slug": slug
                })

            # 5. Create batch product (template)
            batch_template = {
                "name": f"{batch_id}",
                "manufacturer": {
                    "mainURL": "http://localhost"
                },
                "sustainability_metrics_input": sustainability_metrics_input,
                "number_of_units": 1,
                "subparts": []
            }

            create_batch_url = f"{os.environ.get('LEDGER_URL')}/api/products/"
            batch_response = requests.post(create_batch_url, json=batch_template)
            if batch_response.status_code not in (200, 201):
                raise Exception(f"Failed to create batch: {batch_response.status_code} - {batch_response.text}")
            batch_data = batch_response.json()
            slug = batch_data.get('slug')
            if not slug:
                raise Exception('Batch created but no slug returned')
            information_url = f"{os.environ.get('LEDGER_URL')}/api/products/{slug}/"

            # Save batch to database
            execute_db(
                'INSERT INTO batches (id, product_id, information_url, created_at) VALUES (?, ?, ?, ?)',
                [batch_id, product_id, information_url, datetime.utcnow().isoformat()]
            )

            # Create invoices
            for invoice in invoices:
                invoice_id = str(uuid.uuid4())
                execute_db(
                    'INSERT INTO invoices (id, batch_id, facility, organizational_unit, supplier_url, sub_category, invoice_number, invoice_date, emissions_are_per_unit, quantity_needed_per_unit, units_bought, total_amount, currency, transaction_start_date, transaction_end_date, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    [invoice_id, batch_id, invoice['facility'], invoice['organizationalUnit'], invoice['url'], invoice['subCategory'], invoice['invoiceNumber'], invoice['invoiceDate'], invoice['emissionsArePerUnit'], invoice['quantityNeededPerUnit'], invoice['unitsBought'], invoice['totalAmount'], invoice['currency'], invoice['transactionStartDate'], invoice['transactionEndDate'], datetime.utcnow().isoformat()]
                )

            return {
                'message': 'Batch created successfully',
                'productId': product_id,
                'batchId': batch_id
            }, 200

        except Exception as e:
            # Clean up any created batches if error occurs
            execute_db('DELETE FROM batches WHERE id = ?', [batch_id])
            return {'error': f'Error creating batch: {str(e)}'}, 500

@product_ns.route('/product/<product_id>')
@product_ns.param('product_id', 'The product identifier')
class Product(Resource):
    @product_ns.doc('get_product')
    @product_ns.response(200, 'Success', product_detail)
    @product_ns.response(404, 'Product not found')
    @product_ns.response(401, 'Unauthorized')
    @token_required
    def get(self, product_id, current_user):
        """Get product details by ID"""
        # Check if product exists
        product = query_db('SELECT * FROM products WHERE id = ?', [product_id], one=True)
        
        if not product:
            return {'error': 'Product not found'}, 404
        
        # Get all batches for this product
        batches = query_db('SELECT * FROM batches WHERE product_id = ?', [product_id])
        
        # For each batch, fetch information from the URL
        batch_data = []
        
        for batch in batches:
            # In a real application, you would fetch data from the URL
            # For now, let's simulate it
            try:
                # Simulated data (in a real app, you'd call the URL)
                sustainability_data = {
                    'carbon_footprint': 123.45,
                    'water_usage': 67.89,
                    'energy_consumption': 42.0
                }
                
                batch_info = {
                    'id': batch['id'],
                    'created_at': batch['created_at'],
                    **sustainability_data
                }
                
                batch_data.append(batch_info)
            except Exception as e:
                batch_data.append({
                    'id': batch['id'],
                    'error': f'Error fetching data: {str(e)}'
                })
        
        return {
            'productId': product_id,
            'productName': product['name'],
            'batches': batch_data
        }, 200

@product_ns.route('/products')
class ProductList(Resource):
    @product_ns.doc('list_products')
    @product_ns.response(200, 'Success', [product_list_item])
    def get(self):
        """Get all products with their sustainability metrics"""
        async def fetch_all_batch_data(batches):
            async with aiohttp.ClientSession() as session:
                tasks = []
                for batch in batches:
                    tasks.append(fetch_batch_info(session, batch))
                return await asyncio.gather(*tasks)

        async def fetch_batch_info(session, batch):
            try:
                async with session.get(batch['information_url']) as response:
                    if response.status != 200:
                        return []
                    data = await response.json()
                    return data.get('sustainability_metrics', [])  # If sustainability_metrics format is nested like the final format
            except Exception as e:
                print(f"Error fetching batch info: {str(e)}")
                return []

        # Synchronous DB operations
        products = query_db('SELECT * FROM products')
        
        all_products = []
        for product in products:
            batches = query_db('SELECT * FROM batches WHERE product_id = ?', [product['id']])
        
            # Fetch all sustainability records
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            records_lists = loop.run_until_complete(fetch_all_batch_data(batches))
            loop.close()

            # Flatten list of lists
            flat_records = [record for records in records_lists for record in records]

            # Example metadata
            result = {
                "productId": product['id'],
                "productName": product['name'],
                "sustainabilityMetrics": flat_records
            }
            all_products.append(result)

        return all_products, 200

@emissions_ns.route('/emissions')
class Emissions(Resource):
    @emissions_ns.doc('get_emissions')
    @emissions_ns.response(200, 'Success', [emissions_model])
    @emissions_ns.response(404, 'Emissions not found')
    @emissions_ns.response(401, 'Unauthorized')
    @emissions_ns.response(500, 'Internal Server Error')
    # @token_required
    def get(self):
        """
        Endpoint to retrieve emissions data organized by suppliers
        Returns a structured response with supplier details and their emissions data
        """
        try:
            print("Fetching emissions data...")

            subcategories = {
                'Stationary Combustion': 'Scope 1',
                'Mobile Combustion': 'Scope 1',
                'Process Emissions': 'Scope 1',
                'Purchased Electricity': 'Scope 2',
                'Purchased Heat': 'Scope 2',
                'Purchased Steam': 'Scope 2',
                'Purchased Cooling': 'Scope 2',
                'Waste Disposal': 'Scope 3',
                'Business Travel': 'Scope 3',
                'Employee Commuting': 'Scope 3',
                'Purchased Goods and Services': 'Scope 3',
                'Purchased Electricity (Energy)': 'Energy',
                'Water Quantities': 'Water',
                'Water Quality': 'Water',
            }

            def fetch_supplier_data(supplier_url, sub_category):
                try:
                    response = requests.get(supplier_url, timeout=100)
                    if response.status_code != 200:
                        return None
                    data = response.json()
                    data['subCategory'] = sub_category
                    return data
                except Exception as e:
                    print(f"Error fetching supplier data: {str(e)}")
                    return None

            def process_batch_suppliers(batch):
                try:
                    supplier_emissions = []
                    batch['suppliers'] = query_db('SELECT * FROM invoices WHERE batch_id = ?', [batch['id']])
                    if not batch['suppliers']:
                        print(f"No suppliers found for batch {batch['id']}")
                        return []

                    for supplier in batch['suppliers']:
                        if 'supplier_url' in supplier:
                            supplier_data = fetch_supplier_data(supplier['supplier_url'], supplier['sub_category'])
                            if supplier_data:
                                emissions = 0
                                water_consumption = 0
                                energy_consumption = 0
                                metricsArePerUnit = supplier.get('emissionsArePerUnit', 'NO')
                                quantityNeededPerUnit = float(supplier.get('quantityNeededPerUnit', 1))
                                unitsBought = float(supplier.get('unitsBought', 1))
                                if 'sustainability_metrics' in supplier_data:
                                    for metric in supplier_data['sustainability_metrics']:
                                        category = subcategories.get(metric.get('name'), 'Unknown')
                                        if category in ['Scope 1', 'Scope 2', 'Scope 3']:
                                            emissions += metric.get('value', 0) * (quantityNeededPerUnit if metricsArePerUnit == 'YES' else quantityNeededPerUnit / unitsBought)
                                        elif category == 'Water':
                                            water_consumption += metric.get('value', 0) * (quantityNeededPerUnit if metricsArePerUnit == 'YES' else quantityNeededPerUnit / unitsBought)
                                        elif category == 'Energy':
                                            energy_consumption += metric.get('value', 0) * (quantityNeededPerUnit if metricsArePerUnit == 'YES' else quantityNeededPerUnit / unitsBought)

                                print(f"Emissions calculated for {supplier['supplier_url']}: {emissions} kg CO2E, Water: {water_consumption} m3, Energy: {energy_consumption} kWh")
                                emissions_template = {
                                    'product_id': supplier_data.get('product_id'),
                                    'name': supplier_data.get('name'),
                                    'product': supplier_data.get('name'),
                                    'company': supplier_data.get('manufacturer', {}).get('name', ''),
                                    'source': subcategories.get(supplier_data.get('subCategory'), 'Unknown'),
                                    'category': subcategories.get(supplier_data.get('subCategory'), 'Unknown'),
                                    'sub_category': supplier_data.get('subCategory'),
                                    'CO2E': emissions,
                                    'CO2E_unit': 'kg',
                                    'quantity': water_consumption or energy_consumption,
                                    'quantity_unit': 'Cubic meters' if water_consumption else 'kWh',
                                    'timestamp': supplier_data.get('timestamp', datetime.utcnow().isoformat()),
                                    'consumption_end_date': supplier_data.get('consumption_end_date', datetime.utcnow().isoformat()),
                                    'emission_factor': supplier_data.get('emission_factor', 0),
                                    'emission_factor_library': supplier_data.get('emission_factor_library', ''),
                                    'transaction_start_date': supplier_data.get('transaction_start_date', datetime.utcnow().isoformat()),
                                    'transaction_end_date': supplier_data.get('transaction_end_date', datetime.utcnow().isoformat()),
                                    'water_transaction_type': supplier_data.get('water_transaction_type', ''),
                                    'organizational_unit': supplier_data.get('organizational_unit', ''),
                                    'facility': supplier_data.get('facility', ''),
                                }
                                # Append to the emissions list
                                if emissions > 0:
                                    emissions_template['quantity'] = emissions
                                    emissions_template['quantity_unit'] = 'kg'
                                    supplier_emissions.append(emissions_template)
                                # Append water consumption if applicable
                                if water_consumption > 0:
                                    water_template = emissions_template.copy()
                                    water_template['source'] = 'Water'
                                    water_template['category'] = 'Water'
                                    water_template['sub_category'] = 'Water Quantities'
                                    water_template['quantity'] = water_consumption
                                    water_template['quantity_unit'] = 'Cubic meters'
                                    water_template['CO2E'] = 0
                                    supplier_emissions.append(water_template)
                                # Append energy consumption if applicable
                                if energy_consumption > 0:
                                    energy_template = emissions_template.copy()
                                    energy_template['source'] = 'Energy'
                                    energy_template['category'] = 'Energy'
                                    energy_template['sub_category'] = 'Purchased Electricity (Energy)'
                                    energy_template['quantity'] = energy_consumption
                                    energy_template['quantity_unit'] = 'kWh'
                                    energy_template['CO2E'] = 0
                                    supplier_emissions.append(energy_template)

                    return supplier_emissions
                except Exception as e:
                    print(f"Error processing batch suppliers: {str(e)}")
                    return []

            try:
                print("Fetching all products and their emissions data...")
                products = query_db('SELECT * FROM products')
            except Exception as e:
                print(f"Error fetching products: {str(e)}")
                return {'error': 'Error fetching products'}, 500
            all_emissions = []

            for product in products:
                product_id = product['id']
                batches = query_db('SELECT * FROM batches WHERE product_id = ?', [product_id])
                for batch in batches:
                    batch_emissions = process_batch_suppliers(batch)
                    all_emissions.extend(batch_emissions)

            return all_emissions, 200

        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            print(f"Error type: {type(e)}")
            emissions_ns.abort(500, "Internal Server Error")

# Main function to run the Flask app
if __name__ == '__main__':
    app.run(debug=True, port=5000, host='0.0.0.0')
    # Uncomment the following line to run the database setup
    # setup_database()