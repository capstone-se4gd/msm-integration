# routes/invoices.py
from flask_restx import Namespace, Resource
from flask import request
from auth import token_required
from utils.xml_parser import process_multiple_xml_files
from utils.helpers import schedule_transaction_deletion
import uuid
import json
from datetime import datetime, timedelta
from auth import query_db, execute_db
import asyncio
from flask_restx import Api, Resource, fields, Namespace
from concurrent.futures import ThreadPoolExecutor
from flask import current_app
from models import register_models

invoice_ns = Namespace('invoices', description='Invoice processing operations')
models = register_models(invoice_ns)
# Endpoints with Flask-RESTx
@invoice_ns.route('/process-invoices')
class ProcessInvoices(Resource):
    @invoice_ns.doc('process_invoices')
    @invoice_ns.response(202, 'Processing started', models['transaction_response'])
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

                with current_app.app_context():  # Properly enter the application context
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
                with current_app.app_context():  # Context needed here too
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
    @invoice_ns.response(200, 'Success', models['transaction_detail'] )
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
