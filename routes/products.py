from flask_restx import Namespace, Resource
from auth import token_required
from auth import query_db
import asyncio
import aiohttp
from models import register_models

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
    @product_ns.response(200, 'Success', [models['product_list_item']])
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

        return all_products, 200
