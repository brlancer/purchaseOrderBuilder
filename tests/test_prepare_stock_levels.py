import sys
import os

# Add the project directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from fetch_data import fetch_shiphero_stock_levels, fetch_airtable_incoming_stock, fetch_shopify_inventory_data
from transform_data import transform_stock_levels
from utils import export_df

def test_prepare_stock_levels():
    # Prepare stock levels
    stock_levels_data = fetch_shiphero_stock_levels(use_cache=True)
    incoming_stock_data = fetch_airtable_incoming_stock()
    committed_stock_data = fetch_shopify_inventory_data()
    stock_levels_df = transform_stock_levels(stock_levels_data, incoming_stock_data, committed_stock_data)

    # Export the DataFrame to a CSV file
    export_df(stock_levels_df, "test_committed_stock")

# Test the function from command line
if __name__ == "__main__":
    test_prepare_stock_levels()