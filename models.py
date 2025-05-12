from flask_restx import fields

def register_models(api):
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
