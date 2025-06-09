from flask_restx import Namespace, Resource
from auth import token_required
from auth import query_db
import asyncio
import aiohttp
from models import register_models
from datetime import datetime
from flask import request
from extensions import cache

# Add this helper function at the top of your file
def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

product_ns = Namespace('products', description='Product management operations')
models = register_models(product_ns)

@product_ns.route('/product/<product_id>')
@product_ns.param('product_id', 'The product identifier')
class Product(Resource):
    @product_ns.doc('get_product')
    @product_ns.response(200, 'Success', models['product_detail'])
    @product_ns.response(404, 'Product not found')
    @product_ns.response(401, 'Unauthorized')
    @token_required
    def get(self, product_id, current_user):
        """Get product details by ID"""
        # Check if product exists
        product = query_db('SELECT * FROM products WHERE id = %s', [product_id], one=True)
        
        if not product:
            return {'error': 'Product not found'}, 404
        
        # Get all batches for this product
        batches = query_db('SELECT * FROM batches WHERE product_id = %s', [product_id])
        batch_count = len(batches)
        
        # Collect all invoices related to all batches
        all_invoices = []
        for batch in batches:
            invoices = query_db('SELECT * FROM invoices WHERE batch_id = %s', [batch['id']])
            all_invoices.extend(invoices)
        
        # Fetch supplier information for each invoice
        async def fetch_supplier_info(url):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        if response.status == 200:
                            data = await response.json()
                            return data.get('name')
                        return None
            except Exception as e:
                print(f"Error fetching supplier info: {str(e)}")
                return None

        async def fetch_all_suppliers():
            tasks = []
            for invoice in all_invoices:
                if invoice.get('supplier_url'):
                    tasks.append(fetch_supplier_info(invoice['supplier_url']))
            return await asyncio.gather(*tasks)
        
        # Create and execute event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        supplier_names = loop.run_until_complete(fetch_all_suppliers())
        loop.close()
        
        # Filter out None values and get unique names
        unique_suppliers = list(set(filter(None, supplier_names)))
        
        # Convert product name to string if it's a datetime
        product_name = product['name']
        if isinstance(product_name, datetime):
            product_name = product_name.isoformat()
        
        return {
            'productId': product_id,
            'productName': product_name,
            'batchCount': batch_count,
            'relatedSuppliers': unique_suppliers
        }, 200

@product_ns.route('/products')
class ProductList(Resource):
    @product_ns.doc('list_products')
    @product_ns.param('page', 'Page number', type=int, default=1)
    @product_ns.param('per_page', 'Items per page', type=int, default=10)
    @cache.cached(timeout=300, query_string=True)
    def get(self):
        """Get all products with their sustainability metrics (paginated)"""

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

        # Parse pagination parameters
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 10, type=int)
        
        # Limit per_page to prevent excessive loads
        per_page = min(per_page, 50)
        
        # Calculate offset
        offset = (page - 1) * per_page
        
        # Get paginated products
        products = query_db(
            'SELECT * FROM products ORDER BY created_at DESC LIMIT %s OFFSET %s', 
            [per_page, offset]
        )
        
        # Get total count for pagination info
        total = query_db('SELECT COUNT(*) as count FROM products', one=True)['count']
        
        all_products = []
        for product in products:
            batches = query_db('SELECT * FROM batches WHERE product_id = %s', [product['id']])
        
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

        return {
            'products': all_products,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total,
                'pages': (total + per_page - 1) // per_page
            }
        }, 200
