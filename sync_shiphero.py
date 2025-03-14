import requests
from pyairtable import Table
import config
import json
from fetch_data import fetch_purchase_orders_from_shiphero
import config

def prepare_graphql_query_to_create_purchase_orders(po_record):
    """Prepare the GraphQL query for the purchase_order_create mutation."""
    po_number = po_record['fields']['PO #']
    vendor_id = po_record['fields']['ShipHero Vendor ID'][0]
    warehouse_id = config.SHIPHERO_WAREHOUSE_ID

    line_items_data = [
        {
            "sku": item['fields']['sku'][0] if isinstance(item['fields']['sku'], list) else item['fields']['sku'],
            "quantity": item['fields']['Quantity Ordered'],
            "price": f"{item['fields']['Total Unit Cost (active)']:.2f}",
            "expected_weight_in_lbs": "0.0" # Placeholder for now
        }
        for item in po_record['line_items']
    ]

    # Calculate subtotal as the sum of (quantity * price) for all line items
    subtotal = sum([float(item['quantity']) * float(item['price']) for item in line_items_data])

    # Convert line_items_data to a JSON string
    line_items_json = json.dumps(line_items_data)

    # Remove quotes around keys in the JSON string
    line_items_json = line_items_json.replace('"sku"', 'sku').replace('"quantity"', 'quantity').replace('"price"', 'price').replace('"expected_weight_in_lbs"', 'expected_weight_in_lbs')

    query = {
        "query": f"""
        mutation {{
            purchase_order_create(
                data: {{
                    po_number: "{po_number}",
                    vendor_id: "{vendor_id}",
                    warehouse_id: "{warehouse_id}",
                    subtotal: "{subtotal:.2f}",
                    shipping_price: "0.00",
                    total_price: "{subtotal:.2f}",
                    line_items: {line_items_json}
                }}
            ) {{
                request_id
                complexity
                purchase_order {{
                    id
                    fulfillment_status
                    line_items {{
                        edges {{
                            node {{
                                id
                                sku
                                quantity
                                quantity_received
                            }}
                        }}
                    }}
                }}
            }}
        }}
        """
    }

    return query

def execute_shiphero_graphql_query(query):
    """Execute the GraphQL query and return the response."""
    url = "https://public-api.shiphero.com/graphql"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {config.SHIPHERO_API_TOKEN}"
    }
    print("Executing GraphQL query:")
    print(query)
    response = requests.post(url, json=query, headers=headers)
    print("Response status code:", response.status_code)
    print("Response content:", response.content)
    response.raise_for_status()
    return response.json()

def sync_shiphero_to_airtable(purchase_orders_table, line_items_table, airtable_po_record, shiphero_po):
    """Update Airtable with ShipHero Purchase Order data."""
    airtable_po_id = airtable_po_record['id']
    purchase_orders_table.update(airtable_po_id, {
        "shiphero_id": shiphero_po['id'],
        "Status (ShipHero)": shiphero_po['fulfillment_status']
    })

    for airtable_line_item in airtable_po_record['line_items']:
        airtable_sku = airtable_line_item['fields']['sku'][0] if isinstance(airtable_line_item['fields']['sku'], list) else airtable_line_item['fields']['sku']
        shiphero_line_item = next((item['node'] for item in shiphero_po['line_items']['edges'] if item['node']['sku'] == airtable_sku), None)
        if shiphero_line_item:
            shiphero_line_item_id = shiphero_line_item['id']
            quantity_received = shiphero_line_item.get('quantity_received', 0)
            airtable_line_item_id = airtable_line_item['id']
            line_items_table.update(airtable_line_item_id, {
                "shiphero_id": shiphero_line_item_id,
                "Quantity Received": quantity_received
            })

def push_pos_to_shiphero():
    """
    Fetch purchase orders with ShipHero Sync Status = 'Queued' and their associated line items.
    Then push enqueued purchase orders to ShipHero.
    """
    purchase_orders_table = Table(config.AIRTABLE_API_KEY, config.AIRTABLE_PRODUCTION_DEV_BASE_ID, "Purchase Orders")
    line_items_table = Table(config.AIRTABLE_API_KEY, config.AIRTABLE_PRODUCTION_DEV_BASE_ID, "Line Items")

    print("Fetching purchase orders to sync...")
    purchase_orders = purchase_orders_table.all(formula="{ShipHero Sync Status} = 'Queued'")
    print(f"Fetched {len(purchase_orders)} purchase orders to sync.")

    # If no purchase orders are found, return early
    if not purchase_orders:
        print("No purchase orders to sync.")
        return

    for po_record in purchase_orders:
        po_number = po_record['fields']['PO #']
        print(f"Fetching line items for purchase order number: {po_number}...")
        line_items = line_items_table.all(formula=f"AND({{PO #}} = '{po_number}', {{ShipHero Sync Status}} = 'Queued')")
        print(f"Fetched {len(line_items)} line items for purchase order number: {po_number}.")
        po_record['line_items'] = line_items

    for po_record in purchase_orders:
        po_id = po_record['id']
        po_number = po_record['fields']['PO #']

        try:
            # Execute the GraphQL query
            query = prepare_graphql_query_to_create_purchase_orders(po_record)
            response = execute_shiphero_graphql_query(query)
            print(f"Successfully synced purchase order: {po_number} to ShipHero.")
            print(response)

            # Extract ShipHero Purchase Order ID and Line Item IDs
            shiphero_po = response['data']['purchase_order_create']['purchase_order']
            # shiphero_line_items = response['data']['purchase_order_create']['purchase_order']['line_items']['edges']

            # Sync ShipHero Purchase Order ID and Line Item IDs to Airtable
            sync_shiphero_to_airtable(purchase_orders_table, line_items_table, po_record, shiphero_po)

            # Update Airtable status to "Synced"
            purchase_orders_table.update(po_id, {"ShipHero Sync Status": "Synced"})
        except requests.exceptions.RequestException as e:
            print(f"Failed to sync purchase order: {po_number} to ShipHero. Error: {e}")
            # Update Airtable status to "Failed"
            purchase_orders_table.update(po_id, {"ShipHero Sync Status": "Failed"})

def sync_shiphero_purchase_orders_to_airtable(created_from: str = None):
    """
    Syncs purchase orders from ShipHero to Airtable.
    This function performs the following steps:
    1. Fetches purchase orders from Airtable with Status Internal = "Open".
    2. Determines the oldest date created of these open purchase orders.
    3. Fetches purchase orders from ShipHero created after the oldest date created.
    4. Populates the following fields in Airtable for each matching PO in ShipHero:
       - ShipHero PO ID
       - ShipHero Line Item IDs
       - ShipHero Status
       - ShipHero Quantity Received
    5. Prints the number of purchase orders synced.
    6. If any purchase orders were not found in Airtable, prints a warning with the PO numbers.
    Note: Uses Airtable automation to verify whether Status Internal can be updated to "Closed" after syncing.
    """
    # Fetch purchase orders from Airtable with Status Internal = "Open"
    purchase_orders_table = Table(config.AIRTABLE_API_KEY, config.AIRTABLE_PRODUCTION_DEV_BASE_ID, "Purchase Orders")
    line_items_table = Table(config.AIRTABLE_API_KEY, config.AIRTABLE_PRODUCTION_DEV_BASE_ID, "Line Items")
    
    print("Fetching open purchase orders from Airtable...")
    purchase_orders = purchase_orders_table.all(formula="{Status Internal} = 'Open'")
    print(f"Fetched {len(purchase_orders)} open purchase orders from Airtable")
    
    if not purchase_orders:
        print("No open purchase orders found in Airtable.")
        return

    for po_record in purchase_orders:
        po_number = po_record['fields']['PO #']
        print(f"Fetching line items for purchase order number: {po_number}...")
        line_items = line_items_table.all(formula=f"{{PO #}} = '{po_number}'")
        print(f"Fetched {len(line_items)} line items for purchase order number: {po_number}.")
        po_record['line_items'] = line_items

    # Get the oldest date created of purchase orders in Airtable with Status Internal = "Open"
    if not created_from:
        oldest_date_created = min(po['fields']['Date Created'] for po in purchase_orders)
        print(f"Oldest date created for open purchase orders: {oldest_date_created}")
        created_from = oldest_date_created

    # Fetch purchase orders from ShipHero created after the oldest date created
    shiphero_purchase_orders = fetch_purchase_orders_from_shiphero(created_from=created_from)

    if not shiphero_purchase_orders:
        print("No new purchase orders found in ShipHero.")
        return

    # Sync ShipHero purchase orders to Airtable
    synced_count = 0
    not_found_po_numbers = []

    # Iterate over each purchase order fetched from ShipHero
    for shiphero_po in shiphero_purchase_orders:
        po_number = shiphero_po['node']['po_number']
        
        # Find the matching purchase order in Airtable by PO number
        airtable_po_record = next((po for po in purchase_orders if po['fields']['PO #'] == po_number), None)
        
        if airtable_po_record:
            try:
                # Fetch line items for the purchase order
                line_items = line_items_table.all(formula=f"{{PO #}} = '{po_number}'")
                airtable_po_record['line_items'] = line_items

                # Sync ShipHero Purchase Order data to Airtable
                sync_shiphero_to_airtable(purchase_orders_table, line_items_table, airtable_po_record, shiphero_po['node'])
                synced_count += 1
                print(f"Successfully synced purchase order: {po_number} to Airtable.")
            except Exception as e:
                print(f"Failed to sync purchase order: {po_number} to Airtable. Error: {e}")
        else:
            not_found_po_numbers.append(po_number)

    print(f"Synced {synced_count} purchase orders from ShipHero to Airtable.")
    
    if not_found_po_numbers:
        print(f"Warning: The following purchase orders were not found in Airtable: {', '.join(not_found_po_numbers)}")
