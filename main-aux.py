from flask import Flask, request, jsonify, g
import os
import sqlite3
import uuid
import uuid
import json
from datetime import datetime, timedelta
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
    
    required_fields = ['productName', 'productId', 'invoices']
    for field in required_fields:
        if field not in data:
            return jsonify({'error': f'Missing required field: {field}'}), 400
    
    product_id = data.get('productId')
    product_name = data['productName']
    
    if product_id:
        product = query_db('SELECT * FROM products WHERE id = ?', [product_id], one=True)
        if not product:
            return jsonify({'error': 'Product not found'}), 404
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
                    sustainability_metrics[name] += value

    # Run async operations synchronously
    async def fetch_metrics_and_create_batch():
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{os.environ.get('LEDGER_URL')}/api/sustainability-metrics/") as response:
                if response.status != 200:
                    print(f"Failed to fetch sustainability metrics: {response}")
                    raise Exception('Failed to fetch sustainability metrics')
                sustainability_metrics_defined = await response.json()

        sustainability_metrics_input = []
        for metric in sustainability_metrics_defined:
            metric_id = metric['metric_id']
            metric_name = metric['name']
            if metric_name in sustainability_metrics:
                sustainability_metrics_input.append({
                    'metric_id': metric_id,
                    'value': sustainability_metrics[metric_name]
                })

        suppliers = []
        for supplier in invoices:
            # Extract slug from supplier URL
            slug = supplier['url'].split('/')[-1] if '/' in supplier['url'] else ''
            if 'sustainabilityMetrics' in supplier:
                metrics = supplier['sustainabilityMetrics']
                formatted_metrics = []
                for metric in metrics:
                    if 'name' in metric and 'value' in metric:
                        name = metric['name']
                        value = metric['value']
                        # Find the metric_id from the sustainability_metrics_defined list
                        metric_id = next((m['metric_id'] for m in sustainability_metrics_defined if m['name'] == name), None)
                        formatted_metrics.append({
                            'metric_id': metric_id,
                            'value': value
                        })
                supplier['formattedMetrics'] = formatted_metrics
            suppliers.append({
                "name": supplier['productName'],
                "sustainability_metrics_input": supplier['formattedMetrics'],
                "quantity_needed_per_unit": float(supplier['quantityNeededPerUnit']),
                "units_bought": float(supplier['unitsBought']),
                "manufacturer": {
                    "name": "",
                    "mainURL": "http://localhost"
                },
                "slug": slug
            })

        #-----------------------------------------------------------------------------------#
        # Subparts are not used in this example, but have to be included from the suppliers #
        #-----------------------------------------------------------------------------------#
        batch_template = {
            "name": f"{batch_id}",
            "manufacturer": {
                "mainURL": "http://localhost"
            },
            "sustainability_metrics_input": sustainability_metrics_input,
            "number_of_units": 1,
            "subparts": []
        }

        print(f"Batch template: {batch_template}")

        async with aiohttp.ClientSession() as session:
            async with session.post(f"{os.environ.get('LEDGER_URL')}/api/products/", json=batch_template) as response:
                if response.status != 200 and response.status != 201:
                    print(f"Failed to to create batch: {response}")
                    raise Exception('Failed to create batch')
                response_json = await response.json()
                if 'slug' not in response_json:
                    print(f"Failed to create batch: No slug in response")
                    raise Exception('Failed to create batch: No slug in response')
                url_format = f"{os.environ.get('LEDGER_URL')}/api/products/{response_json['slug']}/"
                return url_format

    try:
        information_url = asyncio.run(fetch_metrics_and_create_batch())
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    try:
        execute_db(
            'INSERT INTO batches (id, product_id, information_url, created_at) VALUES (?, ?, ?, ?)',
            [batch_id, product_id, information_url, datetime.utcnow().isoformat()]
        )
    except Exception as e:
        return jsonify({'error': f'Error creating batch: {str(e)}'}), 500
    
    # Create invoices
    try:
        for invoice in invoices:
            invoice_id = str(uuid.uuid4())
            execute_db(
                'INSERT INTO invoices (id, batch_id, facility, organizational_unit, supplier_url, sub_category, invoice_number, invoice_date, emissions_are_per_unit, quantity_needed_per_unit, units_bought, total_amount, currency, transaction_start_date, transaction_end_date, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                [invoice_id, batch_id, invoice['facility'], invoice['organizationalUnit'], invoice['url'], invoice['subCategory'], invoice['invoiceNumber'], invoice['invoiceDate'], invoice['emissionsArePerUnit'], invoice['quantityNeededPerUnit'], invoice['unitsBought'], invoice['totalAmount'], invoice['currency'], invoice['transactionStartDate'], invoice['transactionEndDate'], datetime.utcnow().isoformat()]
            )
    except Exception as e:
        # Rollback batch creation if invoice creation fails
        execute_db(
            'DELETE FROM batches WHERE id = ?',
            [batch_id]
        )
        return jsonify({'error': f'Error creating batch: error saving invoices - {str(e)}'}), 500
    
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

    return jsonify(all_products)

@app.route('/emissions', methods=['GET'])
# @token_required
def get_emissions():
    """
    Endpoint to retrieve emissions data organized by suppliers
    Returns a structured response with supplier details and their emissions data
    """

    # Dictonary of subcategories and their corresponding categories
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

    async def fetch_supplier_data(session, supplier_url, sub_category):
        """Fetch supplier emissions data from the given URL"""
        try:
            async with session.get(supplier_url) as response:
                if response.status != 200:
                    return None
                data = await response.json()
                # Add sub_category to the data
                data['subCategory'] = sub_category
                return data
        except Exception as e:
            print(f"Error fetching supplier data: {str(e)}")
            return None

    async def process_batch_suppliers(batch):
        """Process all suppliers in a batch to collect their emissions data"""
        async with aiohttp.ClientSession() as session:
            tasks = []
            supplier_emissions = []
            # Get all suppliers from the batch
            batch['suppliers'] = query_db('SELECT * FROM suppliers WHERE batch_id = ?', [batch['id']])
            if not batch['suppliers']:
                print(f"No suppliers found for batch {batch['id']}")
                return []
            # Iterate through suppliers
            for supplier in batch['suppliers']:
                print(f"Processing supplier {id}")
                if 'supplier_url' in supplier:
                    # Fetch supplier data asynchronously along with the subCategory
                    tasks.append(fetch_supplier_data(session, supplier['supplier_url'], supplier['sub_category']))
                else:
                    print(f"Supplier URL not found for {supplier['id']}")
            
            # Gather all supplier data
            supplier_data_list = await asyncio.gather(*tasks)
            
            # Process supplier data and format as needed
            for supplier_data in supplier_data_list:
                if supplier_data:

                    # Sum all metrics for the same product_id in its sustainability metrics
                    emissions = 0
                    water_consumption = 0
                    energy_consumption = 0
                    if 'sustainability_metrics' in supplier_data:
                        for metric in supplier_data['sustainability_metrics']:
                            category = subcategories.get(supplier_data.get('subCategory'), 'Unknown')
                            if category == 'Scope 1' or category == 'Scope 2' or category == 'Scope 3':
                                emissions += metric.get('value', 0)
                            elif category == 'Water':
                                water_consumption += metric.get('value', 0)
                            elif category == 'Energy':
                                energy_consumption += metric.get('value', 0)

                    supplier_emissions.append({
                        'product_id': supplier_data.get('product_id'),
                        'name': supplier_data.get('name'),
                        'product': supplier_data.get('name'),
                        'company': supplier_data.get('manufacturer').get('name') if supplier_data.get('manufacturer') else '',
                        'source': subcategories.get(supplier_data.get('subCategory'), 'Unknown'),
                        'category': subcategories.get(supplier_data.get('subCategory'), 'Unknown'),
                        'sub_category': supplier_data.get('subCategory'),
                        'CO2E': emissions,
                        'CO2E_unit': 'kg',
                        'quantity': water_consumption if water_consumption else energy_consumption,
                        'quantity_unit': 'Cubic meters' if water_consumption else 'kWh',
                        'timestamp': supplier_data.get('timestamp') if supplier_data.get('timestamp') else datetime.utcnow().isoformat(),
                        'consumption_end_date': supplier_data.get('consumption_end_date') if supplier_data.get('consumption_end_date') else datetime.utcnow().isoformat(),
                        'emission_factor': supplier_data.get('emission_factor') if supplier_data.get('emission_factor') else 0,
                        'emission_factor_library': supplier_data.get('emission_factor_library') if supplier_data.get('emission_factor_library') else '',
                        'transaction_start_date': supplier_data.get('transaction_start_date') if supplier_data.get('transaction_start_date') else datetime.utcnow().isoformat(),
                        'transaction_end_date': supplier_data.get('transaction_end_date') if supplier_data.get('transaction_end_date') else datetime.utcnow().isoformat(),
                        'water_transaction_type': supplier_data.get('water_transaction_type') if supplier_data.get('water_transaction_type') else '',
                        'organizational_unit': supplier_data.get('organizational_unit') if supplier_data.get('organizational_unit') else '',
                        'facility': supplier_data.get('facility') if supplier_data.get('facility') else '',
                    })
            
            return supplier_emissions

    # Get all products from the database
    products = query_db('SELECT * FROM products')
    all_emissions = []

    # Process each product
    for product in products:
        product_id = product['id']
        batches = query_db('SELECT * FROM batches WHERE product_id = ?', [product_id])
        
        # Setup async loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Process batches concurrently
        all_supplier_emissions = []
        for batch in batches:
            batch_emissions = loop.run_until_complete(process_batch_suppliers(batch))
            all_supplier_emissions.extend(batch_emissions)
        
        loop.close()
        
        # # If there are emissions for this product, add to results
        # if all_supplier_emissions:
        #     emissions_entry = {
        #         'id': product_id,
        #         'suppliers': all_supplier_emissions
        #     }
        #     all_emissions.append(emissions_entry)

        all_emissions.extend(all_supplier_emissions)
    
    return jsonify(all_emissions)

if __name__ == '__main__':
    app.run(debug=True, port=5000, host='0.0.0.0')
    # Uncomment the following line to run the database setup
    # setup_database()