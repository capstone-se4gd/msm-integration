from flask_restx import Namespace, Resource
from auth import token_required
from utils.xml_parser import process_multiple_xml_files
from datetime import datetime, timedelta
from auth import query_db, execute_db
from threading import Thread
import requests

from models import register_models

emissions_ns = Namespace('emissions', description='Emissions data operations')
models = register_models(emissions_ns)

@emissions_ns.route('/emissions')
class Emissions(Resource):
    @emissions_ns.doc('get_emissions')
    @emissions_ns.response(200, 'Success', [models['emissions_model']])
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

            def fetch_supplier_data(supplier_url, supplier):
                try:
                    response = requests.get(supplier_url, timeout=100)
                    if response.status_code != 200:
                        return None
                    data = response.json()
                    data['subCategory'] = supplier.get('sub_category', 'Unknown')
                    data['facility'] = supplier.get('facility', 'Unknown')
                    data['organizational_unit'] = supplier.get('organizational_unit', 'Unknown')
                    data['transaction_start_date'] = supplier.get('transaction_start_date', None)
                    data['transaction_end_date'] = supplier.get('transaction_end_date', None)
                    data['emission_factor'] = supplier.get('emission_factor', None)
                    data['emission_factor_library'] = supplier.get('emission_factor_library', None)
                    data['water_transaction_type'] = supplier.get('water_transaction_type', 'Consumption')
                    data['fuel_type'] = supplier.get('fuel_type', 'Diesel Oil')
                    data['total_amount'] = supplier.get('total_amount', 0)
                    data['currency'] = supplier.get('currency', 'EUR')
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
                            supplier_data = fetch_supplier_data(supplier['supplier_url'], supplier)
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
                                    'name': supplier_data.get('name'),
                                    "originId": supplier_data.get('product_id'),
                                    "productName": supplier_data.get('name'),
                                    "description": f'{supplier_data.get("description", "")}',
                                    "organizationUnit": supplier_data.get('organizational_unit', ''),
                                    'facility': supplier_data.get('facility', ''),
                                    "provider": supplier_data.get('manufacturer', {}).get('name', None),
                                    'quantity': water_consumption or energy_consumption,
                                    "quantityUnit": 'Cubic meters' if water_consumption else 'MWh',
                                    "cost": supplier_data.get('total_amount', 0),
                                    "costUnit": supplier_data.get('currency', 'EUR'),
                                    "emissonSource": 'Carbon emissions',
                                    "emissonCategory": subcategories.get(supplier_data.get('subCategory'), 'Unknown'),
                                    "emissonSubCategory": supplier_data.get('subCategory'),
                                    "CO2E": emissions,
                                    "CO2E_unit": 'kg',
                                    "isRenewable": None,
                                    "timestamp": supplier_data.get('timestamp', datetime.utcnow().isoformat()),
                                    "consumptionStartDate": supplier_data.get('transaction_start_date', datetime.utcnow().isoformat()),
                                    "consumptionEndDate": supplier_data.get('transaction_end_date', datetime.utcnow().isoformat()),
                                    "transactionStartDate": supplier_data.get('transaction_start_date', datetime.utcnow().isoformat()),
                                    "transactionEndDate": supplier_data.get('transaction_end_date', datetime.utcnow().isoformat()),
                                    "emissionFactor": supplier_data.get('emission_factor', None),
                                    "emissionFactorLibrary": supplier_data.get('emission_factor_library', None),
                                    "waterTransactionType": supplier_data.get('water_transaction_type', 'Consumption'),
                                    "fuelType": None,
                                    "dataQualityType": None
                                }
                                # Append to the emissions list
                                if emissions > 0:
                                    emissions_template['quantity'] = emissions
                                    emissions_template['quantityUnit'] = 'kg'
                                    emissions_template['fuelType'] = supplier_data.get('fuel_type', 'Diesel Oil')
                                    supplier_emissions.append(emissions_template)
                                # Append water consumption if applicable
                                if water_consumption > 0:
                                    water_template = emissions_template.copy()
                                    water_template['emissonSource'] = 'Water'
                                    water_template['emissonCategory'] = 'Water'
                                    water_template['emissonSubCategory'] = 'Water Quantities'
                                    water_template['quantity'] = water_consumption
                                    water_template['quantityUnit'] = 'Cubic meters'
                                    water_template['CO2E'] = 0
                                    water_template['fuelType'] = None
                                    supplier_emissions.append(water_template)
                                # Append energy consumption if applicable
                                if energy_consumption > 0:
                                    energy_template = emissions_template.copy()
                                    energy_template['emissonSource'] = 'Energy'
                                    energy_template['emissonCategory'] = 'Energy'
                                    energy_template['emissonSubCategory'] = 'Purchased Electricity (Energy)'
                                    energy_template['quantity'] = energy_consumption
                                    energy_template['quantityUnit'] = 'kWh'
                                    energy_template['emissionFactor'] = 'Facility'
                                    energy_template['CO2E'] = 0
                                    energy_template['fuelType'] = None
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
