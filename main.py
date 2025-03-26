from flask import Flask, jsonify
import requests
from datetime import datetime

app = Flask(__name__)

# Function to fetch data from the API endpoint
def fetch_data(product_slug):
    url = f"http://194.37.81.247:8000/api/products/{product_slug}/"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to retrieve data for {product_slug}, Status Code: {response.status_code}")
        return None

# Function to clean the data and extract the necessary metrics
def clean_data(product_data):
    if not product_data:
        return {}

    cleaned_data = {
        "timestamp": datetime.now().isoformat(),  # Add the current timestamp in ISO format
        "product_id": product_data["product_id"],
        "name": product_data["name"],
        "manufacturer_id": product_data["manufacturer"]["id"],
        "manufacturer_name": product_data["manufacturer"]["name"],
        "sustainability_metrics": []
    }

    # Extracting sustainability metrics from the product
    for metric in product_data.get("sustainability_metrics", []):
        cleaned_data["sustainability_metrics"].append({
            "name": metric["name"],
            "unit": metric["unit"],
            "value": metric["value"]
        })

    # Handling subparts if they exist
    if product_data.get("subparts"):
        subparts_data = []
        for subpart in product_data["subparts"]:
            subpart_data = {
                "subpart_name": subpart["name"],
                "manufacturer_id": subpart["manufacturer"]["id"],
                "manufacturer_name": subpart["manufacturer"]["name"],
                "sustainability_metrics": []
            }
            for metric in subpart.get("sustainability_metrics", []):
                subpart_data["sustainability_metrics"].append({
                    "name": metric["name"],
                    "unit": metric["unit"],
                    "value": metric["value"]
                })
            subparts_data.append(subpart_data)

        # Include subparts data in the final cleaned data
        cleaned_data["subparts"] = subparts_data

    return cleaned_data

# Function to get and clean the data for a given product
def get_cleaned_product_data(product_slug):
    product_data = fetch_data(product_slug)
    cleaned_data = clean_data(product_data)
    return cleaned_data

# Define the endpoint that fetches and cleans the product data
@app.route('/api/products/<product_slug>', methods=['GET'])
def get_product_data(product_slug):
    cleaned_product = get_cleaned_product_data(product_slug)
    if cleaned_product:
        return jsonify(cleaned_product), 200
    else:
        return jsonify({"error": "Product data not found"}), 404

# Start the Flask app
if __name__ == '__main__':
    app.run(debug=True)
