from flask import Flask, jsonify
import requests
from datetime import datetime
import os

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
    url = f"https://msm-integration.onrender.com/api/products/{product_slug}"
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
    
    timestamp = input_json.get("timestamp", datetime.utcnow().isoformat())

    # Extract product emissions, energy, and water into one list
    product_emissions = []
    
    for metric in input_json.get("sustainability_metrics", []):
        if metric["name"] in ["Scope 1", "Scope 2", "Scope 3"]:
            product_emissions.append({
                "scope": metric["name"],
                "category": CATEGORY_MAPPING.get(metric["name"], "Other"),
                "activity": ACTIVITY_MAPPING.get(metric["name"], "General"),
                "value": metric["value"],
                "unit": metric["unit"],
                "timestamp": timestamp,
                "source": "Product"
            })
        elif metric["name"] in ["Energy Usage", "Water Consumption"]:
            product_emissions.append({
                "scope": metric["name"],
                "category": CATEGORY_MAPPING.get(metric["name"], "Other"),
                "activity": "General",
                "value": metric["value"],
                "unit": metric["unit"],
                "timestamp": timestamp,
                "source": "Product"
            })

    # Extract subpart emissions
    subpart_emissions = []
    
    for subpart in input_json.get("subparts", []):
        for metric in subpart.get("sustainability_metrics", []):
            if metric["name"] in ["Scope 1", "Scope 2", "Scope 3"]:
                subpart_emissions.append({
                    "scope": metric["name"],
                    "category": CATEGORY_MAPPING.get(metric["name"], "Other"),
                    "activity": ACTIVITY_MAPPING.get(metric["name"], "Subpart Emission"),
                    "value": metric["value"],
                    "unit": metric["unit"],
                    "timestamp": timestamp,
                    "source": "Subpart",
                    "subpart_name": subpart.get("subpart_name", "Unknown")
                })
            elif metric["name"] in ["Energy Usage", "Water Consumption"]:
                subpart_emissions.append({
                    "scope": metric["name"],
                    "category": CATEGORY_MAPPING.get(metric["name"], "Other"),
                    "activity": "Subpart Emission",
                    "value": metric["value"],
                    "unit": metric["unit"],
                    "timestamp": timestamp,
                    "source": "Subpart",
                    "subpart_name": subpart.get("subpart_name", "Unknown")
                })

    # Build final structured output without separate energy/water sections
    return {
        "product_emissions": product_emissions,
        "subpart_emissions": subpart_emissions
    }

# Define the endpoint
@app.route('/api/products/<product_slug>', methods=['GET'])
def get_transformed_data(product_slug):
    product_data = fetch_data(product_slug)
    transformed_data = transform_data(product_data)
    
    if transformed_data:
        return jsonify(transformed_data), 200
    else:
        return jsonify({"error": "Product data not found"}), 404

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))  # Default to 5000 if PORT is not set
    app.run(host="0.0.0.0", port=port)