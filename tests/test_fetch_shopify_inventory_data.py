import sys
import os

# Add the project directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the function to test
from fetch_data import fetch_shopify_inventory_data
from utils import export_json

def test_fetch_shopify_inventory_data():
    # Fetch Shopify inventory data
    inventory_data = fetch_shopify_inventory_data()
    # Save the response to a JSON file in the output subdirectory 
    export_json(inventory_data, "Shopify Inventory Data")

# Test the function from command line
if __name__ == "__main__":
    test_fetch_shopify_inventory_data()