from flask_restx import Namespace, Resource
from flask import request
from auth import token_required
import uuid
from datetime import datetime
from auth import query_db, execute_db
import requests
import os
from models import register_models

batch_ns = Namespace('batches', description='Batch management operations')
models = register_models(batch_ns)
@batch_ns.route('/create-batch')
class CreateBatch(Resource):
    @batch_ns.doc('create_batch')
    @batch_ns.expect(models['batch_request'])
    @batch_ns.response(200, 'Success', models['batch_response'])
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
                'INSERT INTO products (id, name, user_id, created_at) VALUES (?, ?, ?, ?)',
                [product_id, product_name, current_user['id'], datetime.utcnow().isoformat()]
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

@batch_ns.route('/batches')
class BatchList(Resource):
    @batch_ns.doc('list_batches')
    @batch_ns.response(200, 'Success')
    @batch_ns.response(500, 'Internal server error')
    @batch_ns.response(401, 'Unauthorized')
    @token_required
    def get(self, current_user):
        """Retrieve all batches associated with the logged-in user's products"""
        try:
            # Query all batches and their products for the current user
            batches = query_db('''
                SELECT b.id, b.product_id, p.name as product_name, 
                       b.information_url, b.created_at
                FROM batches b
                JOIN products p ON b.product_id = p.id
                WHERE p.user_id = ?
                ORDER BY b.created_at DESC
            ''', [current_user['id']])
            
            return {'batches': batches}, 200
        except Exception as e:
            return {'error': f'Error retrieving batches: {str(e)}'}, 500

@batch_ns.route('/batches/<string:id>')
class BatchDetail(Resource):
    @batch_ns.doc('get_batch')
    @batch_ns.response(200, 'Success')
    @batch_ns.response(404, 'Batch not found')
    @batch_ns.response(403, 'Forbidden - not your batch')
    @batch_ns.response(500, 'Internal server error')
    @batch_ns.response(401, 'Unauthorized')
    @token_required
    def get(self, id, current_user):
        """Retrieve detailed information about a specific batch owned by the user"""
        try:
            # Get batch information with user check
            batch = query_db('''
                SELECT b.id, b.product_id, p.name as product_name, 
                       b.information_url, b.created_at
                FROM batches b
                JOIN products p ON b.product_id = p.id
                WHERE b.id = ? AND p.user_id = ?
            ''', [id, current_user['id']], one=True)
            
            if not batch:
                # Check if batch exists at all
                exists = query_db('SELECT 1 FROM batches WHERE id = ?', [id], one=True)
                if exists:
                    return {'error': 'Access denied to this batch'}, 403
                else:
                    return {'error': 'Batch not found'}, 404
            
            # Get batch invoices
            invoices = query_db('''
                SELECT id, facility, organizational_unit, supplier_url as url, 
                       sub_category as subCategory, invoice_number as invoiceNumber, invoice_date as invoiceDate, 
                       emissions_are_per_unit as emissionsArePerUnit, quantity_needed_per_unit as quantityNeededPerUnit, 
                       units_bought as unitsBought, total_amount as totalAmount, currency, 
                       transaction_start_date as transactionStartDate, transaction_end_date as transactionEndDate, 
                       created_at as createdAt
                FROM invoices
                WHERE batch_id = ?
            ''', [id])
            
            # Fetch additional data from invoice URLs and batch information_url
            enriched_invoices = []
            for invoice in invoices:
                invoice_data = invoice.copy()
                
                # Fetch supplier data if URL is available
                if invoice['url']:
                    try:
                        supplier_response = requests.get(invoice['url'])
                        if supplier_response.status_code == 200:
                            invoice_data['supplierDetails'] = supplier_response.json()
                    except Exception as e:
                        invoice_data['supplierFetchError'] = str(e)
                
                enriched_invoices.append(invoice_data)
            
            # Get batch data from information_url if available
            batch_data = {}
            if batch['information_url']:
                try:
                    batch_response = requests.get(batch['information_url'])
                    if batch_response.status_code == 200:
                        batch_data = batch_response.json()
                except Exception as e:
                    batch_data = {'fetchError': str(e)}
            
            result = {
                'batch': batch,
                'batchData': batch_data,
                'invoices': enriched_invoices
            }
            
            return result, 200
        except Exception as e:
            return {'error': f'Error retrieving batch details: {str(e)}'}, 500