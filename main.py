from flask import Flask, jsonify
import requests
from datetime import datetime
import os
import json

app = Flask(__name__)

# MSM Mapping
CATEGORY_MAPPING = {
    "Scope 1": "Stationary Combustion",
    "Scope 2": "Purchased Electricity",
    "Scope 3": "Purchased Goods and Services",
    "Energy Usage": "Purchased Electricity",
    "Water Consumption": "Water Quantities"
}

ACTIVITY_MAPPING = {
    "Scope 1": "Manufacturing Process",
    "Scope 2": "Grid Electricity",
    "Scope 3": "Raw Material Procurement"
}

# Function to fetch data from the API endpoint
def fetch_data(product_slug):
    url = f"http://194.37.81.247:8000/api/products/{product_slug}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to retrieve data for {product_slug}, Status Code: {response.status_code}")
        return None

# Function to transform the JSON structure
def transform_data(input_json):
    if not input_json:
        return {}
    
    timestamp = datetime.utcnow().isoformat()
    
    product_id = input_json.get("product_id", "Unknown")
    manufacturer_name = input_json.get("manufacturer", {}).get("name", "Unknown")
    
    # Extract product emissions, energy, and water into one list
    product_emissions = []
    
    for metric in input_json.get("sustainability_metrics", []):
        product_emissions.append({
            "scope": metric["name"],
            "category": CATEGORY_MAPPING.get(metric["name"], "Other"),
            "activity": ACTIVITY_MAPPING.get(metric["name"], "General"),
            "value": metric["value"],
            "unit": metric["unit"],
            "timestamp": timestamp,
            "source": "Product",
            "product_id": product_id,
            "manufacturer_name": manufacturer_name
        })
    
    # Extract subpart emissions
    subpart_emissions = []
    
    for subpart in input_json.get("subparts", []):
        subpart_id = subpart.get("subpart_id", "Unknown")
        subpart_name = subpart.get("name", "Unknown")
        subpart_manufacturer_name = subpart.get("manufacturer", {}).get("name", "Unknown")
        
        for metric in subpart.get("sustainability_metrics", []):
            subpart_emissions.append({
                "scope": metric["name"],
                "category": CATEGORY_MAPPING.get(metric["name"], "Other"),
                "activity": ACTIVITY_MAPPING.get(metric["name"], "Subpart Emission"),
                "value": metric["value"],
                "unit": metric["unit"],
                "timestamp": timestamp,
                "source": "Subpart",
                "subpart_id": subpart_id,
                "subpart_name": subpart_name,
                "manufacturer_name": subpart_manufacturer_name
            })
    
    # Build final structured output
    return {
        "product_emissions": product_emissions,
        "subpart_emissions": subpart_emissions
    }
def load_json_from_file(filename):
    if not os.path.exists(filename):
        return {"error": "File not found"}
    
    with open(filename, "r", encoding="utf-8") as file:
        data = json.load(file)
    return data

# Define the endpoint
@app.route('/api/products/<product_slug>', methods=['GET'])
def get_transformed_data(product_slug):
    product_data = fetch_data(product_slug)
    transformed_data = transform_data(product_data)
    
    if transformed_data:
        return jsonify(transformed_data), 200
    else:
        return jsonify({"error": "Product data not found"}), 404
    
@app.route('/api/products/test-data', methods=['GET'])
def get_json_data():
    json_data = load_json_from_file("test-data.json")
    return jsonify(json_data)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))  # Default to 5000 if PORT is not set
    app.run(host="0.0.0.0", port=port)