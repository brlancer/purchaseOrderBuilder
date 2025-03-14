import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from pyairtable import Table
import pandas as pd
import config

# Initialize gspread and authenticate with Google Sheets

# Define the scope
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']

# Path to your service account key file
SERVICE_ACCOUNT_FILE = 'service-account.json'  # Update this path

# Authenticate and create the service
credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
gc = gspread.authorize(credentials)

def get_record_ids_by_value(table, field, values):
    """Fetch the record IDs for the given field values from the specified table."""
    print(f"Fetching all records from the {table.name} table...")
    all_records = table.all()
    print(f"Fetched {len(all_records)} records from the {table.name} table.")

    record_ids = {}
    for record in all_records:
        field_value = record['fields'].get(field)
        if field_value in values:
            record_ids[field_value] = record['id']
    
    return record_ids

def populate_production():
    file_id = '1L35Drb5FZfPsV7kk73wZzsqCQ9k6x7KSoKJMYFhefeQ'  # Google Drive file name: PO BUILDER 3.0

    # Open the template file with gspread
    sh = gc.open_by_key(file_id)
    worksheet = sh.worksheet("Replenishment")

    # Fetch the replenishment quantities from the Google Sheet
    expected_headers = ["product_num", "sku", "To Order Qty", "Total Units to Order for this Product"]
    replenishment_data = worksheet.get_all_records(expected_headers=expected_headers)
    replenishment_df = pd.DataFrame(replenishment_data)

    # Remove all rows with 0 or blank in 'Total Units to Order for this Product' column
    replenishment_df = replenishment_df[replenishment_df['Total Units to Order for this Product'] != 0]
    replenishment_df = replenishment_df[replenishment_df['Total Units to Order for this Product'] != '']

    # Keep only the necessary columns
    replenishment_df = replenishment_df[['product_num', 'sku', 'To Order Qty']]

    # Replace empty or invalid values in 'To Order Qty' with 0
    replenishment_df['To Order Qty'] = pd.to_numeric(replenishment_df['To Order Qty'], errors='coerce').fillna(0)

    # Print first 5 rows of the DataFrame
    print(replenishment_df.head())

    # Get the most recent PO # from the Purchase Orders table in the Production base
    print("Fetching the most recent PO #...")
    purchase_orders_table = Table(config.AIRTABLE_API_KEY, config.AIRTABLE_PRODUCTION_DEV_BASE_ID, "Purchase Orders")
    purchase_orders = purchase_orders_table.all(view='Active')
    po_numbers = [int(po['fields']['PO #']) for po in purchase_orders]
    po_numbers.sort()
    latest_po_number = po_numbers[-1] if po_numbers else 0
    print(f"Most recent PO #: {latest_po_number}")

    # Initialize the Line Items table and Products table
    line_items_table = Table(config.AIRTABLE_API_KEY, config.AIRTABLE_PRODUCTION_DEV_BASE_ID, "Line Items")
    variants_table = Table(config.AIRTABLE_API_KEY, config.AIRTABLE_PRODUCTION_DEV_BASE_ID, "Variants")
    products_table = Table(config.AIRTABLE_API_KEY, config.AIRTABLE_PRODUCTION_DEV_BASE_ID, "Products")

    # Get the record IDs for the SKUs in the Variants table
    print("Fetching record IDs for SKUs from the Variants table...")
    skus = replenishment_df['sku'].unique()
    variant_record_ids = get_record_ids_by_value(variants_table, 'SKU', skus)
    print(f"Found record IDs for {len(variant_record_ids)} SKUs.")

    # Get the record IDs for the product numbers in the Products table
    print("Fetching record IDs for product numbers from the Products table...")
    product_nums = replenishment_df['product_num'].unique()
    product_record_ids = get_record_ids_by_value(products_table, 'Product Number', product_nums)
    print(f"Found record IDs for {len(product_record_ids)} product numbers.")

    # Create new purchase orders for each unique product_num
    new_po_records = []
    unique_product_nums = replenishment_df['product_num'].unique()
    for product_num in unique_product_nums:
        # Increment the PO number
        latest_po_number += 1
        po_number = latest_po_number

        # Create a new purchase order record
        new_po_record = {
            "PO #": str(po_number),
            "Product": [product_record_ids.get(product_num)],
            "Line Items": []  # This will be populated later
        }
        new_po_records.append(new_po_record)

    # Add new purchase order records to the Purchase Orders table
    print("Adding new purchase order records to the Purchase Orders table...")
    purchase_orders_table.batch_create(new_po_records)
    print("Added new purchase order records.")

    # Fetch the newly created purchase order records to get their IDs
    print("Fetching newly created purchase order records...")
    new_po_numbers = [po['PO #'] for po in new_po_records]
    new_po_record_ids = get_record_ids_by_value(purchase_orders_table, 'PO #', new_po_numbers)
    print(f"Found record IDs for {len(new_po_record_ids)} new purchase orders.")

    # Create new line items
    new_line_item_records = []
    for index, row in replenishment_df.iterrows():
        product_num = row['product_num']
        sku = row['sku']
        to_order_qty = row['To Order Qty']

        # Get the corresponding purchase order ID
        po_number = new_po_numbers[unique_product_nums.tolist().index(product_num)]
        po_record_id = new_po_record_ids.get(po_number)

        # Create a new line item record
        new_line_item_record = {
            "Purchase Order": [po_record_id],
            "Variant": [variant_record_ids.get(sku)],
            "Quantity Ordered": to_order_qty
        }
        new_line_item_records.append(new_line_item_record)

    # Print the first 5 new line item records
    print(new_line_item_records[:5])

    # Add new line item records to the Line Items table
    print("Adding new line item records to the Line Items table...")
    line_items_table.batch_create(new_line_item_records)
    print("Added new line item records.")

