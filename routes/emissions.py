from flask_restx import Namespace, Resource
from datetime import datetime, date
from auth import query_db
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from extensions import cache

from models import register_models

emissions_ns = Namespace('emissions', description='Emissions data operations')
models = register_models(emissions_ns)

def format_date(date_obj):
    """Convert date objects to ISO format strings"""
    if date_obj is None:
        return None
    if isinstance(date_obj, datetime):
        return date_obj.isoformat()
    if isinstance(date_obj, date):
        return date_obj.isoformat()
    return date_obj  # Return as is if it's already a string or other type

def fetch_and_process_supplier(supplier, subcategories):
    """Process a single supplier synchronously"""
    try:
        # Make HTTP request with timeout
        response = requests.get(supplier['supplier_url'], timeout=10)
        
        if response.status_code != 200:
            print(f"HTTP {response.status_code} for {supplier['supplier_url']}")
            return []
        
        supplier_data = response.json()
        
        # Process emissions data
        emissions = 0
        water_consumption = 0
        energy_consumption = 0
        metricsArePerUnit = supplier.get('emissions_are_per_unit', 'NO')
        quantityNeededPerUnit = float(supplier.get('quantity_needed_per_unit', 1))
        unitsBought = float(supplier.get('units_bought', 1))
        
        if 'sustainability_metrics' in supplier_data:
            for metric in supplier_data['sustainability_metrics']:
                category = subcategories.get(metric.get('name'), 'Unknown')
                metric_value = metric.get('value', 0)
                multiplier = quantityNeededPerUnit if metricsArePerUnit == 'YES' else quantityNeededPerUnit / unitsBought
                
                if category in ['Scope 1', 'Scope 2', 'Scope 3']:
                    emissions += metric_value * multiplier
                elif category == 'Water':
                    water_consumption += metric_value * multiplier
                elif category == 'Energy':
                    energy_consumption += metric_value * multiplier
                
        results = []
        base_template = {
            'name': supplier_data.get('name'),
            "originId": supplier_data.get('product_id'),
            "productName": supplier_data.get('name'),
            "description": f'{supplier_data.get("description", "")}',
            "organizationUnit": supplier.get('organizational_unit', ''),
            'facility': supplier.get('facility', ''),
            "provider": supplier_data.get('manufacturer', {}).get('name', None),
            "cost": supplier.get('total_amount', 0),
            "costUnit": supplier.get('currency', 'EUR'),
            "timestamp": supplier_data.get('timestamp', datetime.utcnow().isoformat()),
            "consumptionStartDate": format_date(supplier.get('transaction_start_date')) or datetime.utcnow().isoformat(),
            "consumptionEndDate": format_date(supplier.get('transaction_end_date')) or datetime.utcnow().isoformat(),
            "transactionStartDate": format_date(supplier.get('transaction_start_date')) or datetime.utcnow().isoformat(),
            "transactionEndDate": format_date(supplier.get('transaction_end_date')) or datetime.utcnow().isoformat(),
            "emissionFactor": supplier.get('emission_factor', None),
            "emissionFactorLibrary": supplier.get('emission_factor_library', None),
            "waterTransactionType": supplier.get('water_transaction_type', 'Consumption'),
            "dataQualityType": None
        }
        
        # Add emissions if applicable
        if emissions > 0:
            emissions_template = base_template.copy()
            emissions_template.update({
                'quantity': emissions,
                'quantityUnit': 'kg',
                'emissonSource': 'Carbon emissions',
                'emissonCategory': subcategories.get(supplier.get('sub_category'), 'Unknown'),
                'emissonSubCategory': supplier.get('sub_category'),
                'CO2E': emissions,
                'CO2E_unit': 'kg',
                'isRenewable': None,
                'fuelType': supplier.get('fuel_type', 'Diesel Oil')
            })
            results.append(emissions_template)
        
        # Add water consumption if applicable
        if water_consumption > 0:
            water_template = base_template.copy()
            water_template.update({
                'emissonSource': 'Water',
                'emissonCategory': 'Water',
                'emissonSubCategory': 'Water Quantities',
                'quantity': water_consumption,
                'quantityUnit': 'Cubic meters',
                'CO2E': 0,
                'CO2E_unit': 'kg',
                'isRenewable': None,
                'fuelType': None
            })
            results.append(water_template)
        
        # Add energy consumption if applicable
        if energy_consumption > 0:
            energy_template = base_template.copy()
            energy_template.update({
                'emissonSource': 'Energy',
                'emissonCategory': 'Energy',
                'emissonSubCategory': 'Purchased Electricity (Energy)',
                'quantity': energy_consumption,
                'quantityUnit': 'kWh',
                'emissionFactor': 'Facility',
                'CO2E': 0,
                'CO2E_unit': 'kg',
                'isRenewable': None,
                'fuelType': None
            })
            results.append(energy_template)

        return results
    
    except requests.exceptions.Timeout:
        print(f"Timeout error for supplier {supplier.get('id')}: {supplier.get('supplier_url')}")
        return []
    except requests.exceptions.RequestException as e:
        print(f"HTTP request error for supplier {supplier.get('id')}: {str(e)}")
        return []
    except Exception as e:
        print(f"Error processing supplier {supplier.get('id')}: {str(e)}")
        return []

@emissions_ns.route('/emissions')
class Emissions(Resource):
    @emissions_ns.doc('get_emissions')
    @cache.cached(timeout=600)  # Cache for 10 minutes
    def get(self):
        """Endpoint to retrieve emissions data organized by suppliers"""
        try:            
            # Define subcategories for emissions
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
            
            # Batch fetch product data
            products = query_db('SELECT * FROM products')
            
            # Prepare batch queries to reduce database calls
            product_ids = [product['id'] for product in products]
            
            if not product_ids:
                return [], 200
            
            # Single query to get all relevant batches
            batches_query = 'SELECT * FROM batches WHERE product_id IN ({})'.format(
                ','.join(['%s'] * len(product_ids))
            )
            
            all_batches = query_db(batches_query, product_ids)
            batch_ids = [batch['id'] for batch in all_batches]
            
            # Get all invoices in one query
            if batch_ids:
                invoices_query = 'SELECT * FROM invoices WHERE batch_id IN ({})'.format(
                    ','.join(['%s'] * len(batch_ids))
                )
                all_invoices = query_db(invoices_query, batch_ids)
            else:
                all_invoices = []
            
            # Filter invoices that have supplier URLs
            valid_invoices = [
                invoice for invoice in all_invoices 
                if 'supplier_url' in invoice and invoice['supplier_url']
            ]
            
            if not valid_invoices:
                return [], 200
            
            # Process suppliers concurrently using ThreadPoolExecutor
            all_emissions = []
            
            # Use ThreadPoolExecutor for concurrent HTTP requests
            with ThreadPoolExecutor(max_workers=10) as executor:
                # Submit all tasks
                future_to_supplier = {
                    executor.submit(fetch_and_process_supplier, invoice, subcategories): invoice
                    for invoice in valid_invoices
                }
                
                # Collect results as they complete
                for future in as_completed(future_to_supplier):
                    try:
                        emissions_list = future.result()
                        if emissions_list:
                            all_emissions.extend(emissions_list)
                    except Exception as e:
                        supplier = future_to_supplier[future]
                        print(f"Error processing supplier {supplier.get('id')}: {str(e)}")
            
            print(f"Total emissions records processed: {len(all_emissions)}")
            return all_emissions, 200
            
        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"error": "Internal Server Error"}, 500