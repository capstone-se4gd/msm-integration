from flask import Flask, request, jsonify, g
import sqlite3
import uuid
import json
import datetime
import asyncio
import aiohttp
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
import threading
from functools import wraps

# Import the authentication module
from auth import register_auth_routes, token_required, query_db, execute_db

app = Flask(__name__)

# Database file path
DATABASE = 'database.db'

# Register authentication routes
app = register_auth_routes(app)

# Schedule transaction deletion
def schedule_transaction_deletion(transaction_id, hours=24):
    """Schedule transaction to be deleted after specified hours."""
    deletion_time = datetime.datetime.utcnow() + datetime.timedelta(hours=hours)
    
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
    except Exception as e:
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

            print(f"Fetched sustainability data: {sustainability_data}")
            
            # Add sustainability metrics to JSON data
            if 'error' not in sustainability_data:
                json_data['sustainabilityMetrics'] = sustainability_data
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

# Endpoints
@app.route('/process-invoices', methods=['POST'])
@token_required
def process_invoices(current_user):
    # Check if files were uploaded
    if 'files' not in request.files:
        return jsonify({'error': 'No files provided'}), 400
    
    files = request.files.getlist('files')
    if not files:
        return jsonify({'error': 'No files provided'}), 400
    
    # Read all XML files
    xml_contents = []
    for file in files:
        print(f"Processing file: {file.filename}")
        if file.filename.endswith('.xml'):
            xml_contents.append(file.read().decode('utf-8'))
    
    if not xml_contents:
        return jsonify({'error': 'No XML files provided'}), 400
    
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
                            [transaction_id, json.dumps(results), datetime.datetime.utcnow().isoformat()]
                        )
                    # Save results to database and set deletion time to 24 hours later
                    execute_db(
                        update,
                        [json.dumps(results), (datetime.datetime.utcnow() + datetime.timedelta(hours=24)).isoformat(), transaction_id]
                    )
                except Exception as e:
                    print(f"Error saving transaction {transaction_id}: {e}")
                    execute_db(
                        update,
                        [f'Error saving transaction: {str(e)}', (datetime.datetime.utcnow() + datetime.timedelta(hours=24)).isoformat(), transaction_id]
                    )
                    
            return True

        except Exception as e:
            print(f"Error processing transaction {transaction_id}: {e}")
            with app.app_context():  # Context needed here too
                execute_db(
                    update,
                    [transaction_id, f'Error processing files: {str(e)}', datetime.datetime.utcnow().isoformat()]
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
            [transaction_id, 'Processing started', datetime.datetime.utcnow().isoformat()]
        )
    
    return jsonify({
        'message': 'Processing started',
        'transaction_id': transaction_id
    }), 202

@app.route('/transaction/<transaction_id>', methods=['GET'])
@token_required
def get_transaction(current_user, transaction_id):
    # Get transaction from database
    transaction = query_db('SELECT * FROM transactions WHERE id = ?', [transaction_id], one=True)
    
    if not transaction:
        return jsonify({'error': 'Transaction not found'}), 404
    
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
    
    return jsonify({
        'id': transaction_id,
        'result': result,
        'created_at': transaction['created_at'],
        'deletion_scheduled_at': transaction['deletion_scheduled_at']
    })

@app.route('/create-batch', methods=['POST'])
@token_required
def create_batch(current_user):
    data = request.get_json()
    
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    # Check required fields
    required_fields = ['productName', 'xmlData', 'sustainabilityMetrics']
    for field in required_fields:
        if field not in data:
            return jsonify({'error': f'Missing required field: {field}'}), 400
    
    product_id = data.get('productId')
    product_name = data['productName']
    
    # Check if product exists or create a new one
    if product_id:
        product = query_db('SELECT * FROM products WHERE id = ?', [product_id], one=True)
        if not product:
            return jsonify({'error': 'Product not found'}), 404
    else:
        # Create new product
        product_id = str(uuid.uuid4())
        execute_db(
            'INSERT INTO products (id, name, created_at) VALUES (?, ?, ?)',
            [product_id, product_name, datetime.datetime.utcnow().isoformat()]
        )
    
    # Create batch
    batch_id = str(uuid.uuid4())
    
    # In a real application, you would POST to the external endpoint
    # For now, let's simulate it and create an URL
    information_url = f"https://api.example.com/sustainability/{batch_id}"
    
    # Save batch
    execute_db(
        'INSERT INTO batches (id, product_id, information_url, created_at) VALUES (?, ?, ?, ?)',
        [batch_id, product_id, information_url, datetime.datetime.utcnow().isoformat()]
    )
    
    return jsonify({
        'message': 'Batch created successfully',
        'productId': product_id,
        'batchId': batch_id
    })

@app.route('/product/<product_id>', methods=['GET'])
@token_required
def get_product(current_user, product_id):
    # Check if product exists
    product = query_db('SELECT * FROM products WHERE id = ?', [product_id], one=True)
    
    if not product:
        return jsonify({'error': 'Product not found'}), 404
    
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
    
    return jsonify({
        'productId': product_id,
        'productName': product['name'],
        'batches': batch_data
    })

@app.route('/products', methods=['GET'])
# @token_required
def get_products():
    # Get all products
    products = query_db('SELECT * FROM products')
    
    result = []
    
    for product in products:
        # Get all batches for this product
        batches = query_db('SELECT * FROM batches WHERE product_id = ?', [product['id']])
        
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
        
        result.append({
            'productId': product['id'],
            'productName': product['name'],
            'batches': batch_data
        })
    
    return jsonify(result)

@app.route('/hello-world', methods=['GET'])
def hello_world():
    return jsonify({'message': 'Hello, World!'})

if __name__ == '__main__':
    app.run(debug=True, port=5000, host='localhost')
    # Uncomment the following line to run the database setup
    # setup_database()