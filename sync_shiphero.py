import requests
from pyairtable import Table
import config
import json

def fetch_purchase_orders_to_sync():
    """Fetch purchase orders with ShipHero Sync Status = 'Queued' and their associated line items."""
    print("Initializing Airtable tables...")
    purchase_orders_table = Table(config.AIRTABLE_API_KEY, config.AIRTABLE_PRODUCTION_DEV_BASE_ID, "Purchase Orders")
    line_items_table = Table(config.AIRTABLE_API_KEY, config.AIRTABLE_PRODUCTION_DEV_BASE_ID, "Line Items")

    print("Fetching purchase orders to sync...")
    purchase_orders = purchase_orders_table.all(formula="{ShipHero Sync Status} = 'Queued'")
    print(f"Fetched {len(purchase_orders)} purchase orders to sync.")

    for po_record in purchase_orders:
        po_number = po_record['fields']['PO #']
        print(f"Fetching line items for purchase order number: {po_number}...")
        line_items = line_items_table.all(formula=f"AND({{PO #}} = '{po_number}', {{ShipHero Sync Status}} = 'Queued')")
        print(f"Fetched {len(line_items)} line items for purchase order number: {po_number}.")
        po_record['line_items'] = line_items

    return purchase_orders

def prepare_graphql_query(po_record):
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

def execute_graphql_query(query):
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

def update_airtable_status(purchase_orders_table, po_id, status):
    """Update the ShipHero Sync Status field in Airtable."""
    print(f"Updating ShipHero Sync Status for purchase order ID: {po_id} to {status}...")
    purchase_orders_table.update(po_id, {"ShipHero Sync Status": status})
    print(f"Updated ShipHero Sync Status for purchase order ID: {po_id}.")

def update_airtable_with_shiphero_ids(purchase_orders_table, line_items_table, po_record, shiphero_po_id, shiphero_line_items):
    """Update Airtable with ShipHero Purchase Order ID and Line Item IDs."""
    po_id = po_record['id']
    print(f"Updating Airtable with ShipHero Purchase Order ID: {shiphero_po_id} for PO ID: {po_id}...")
    purchase_orders_table.update(po_id, {"shiphero_id": shiphero_po_id})
    print(f"Updated Airtable with ShipHero Purchase Order ID: {shiphero_po_id} for PO ID: {po_id}.")

    for line_item in po_record['line_items']:
        airtable_sku = line_item['fields']['sku'][0] if isinstance(line_item['fields']['sku'], list) else line_item['fields']['sku']
        shiphero_line_item_id = next((item['node']['id'] for item in shiphero_line_items if item['node']['sku'] == airtable_sku), None)
        if shiphero_line_item_id:
            line_item_id = line_item['id']
            print(f"Updating Airtable with ShipHero Line Item ID: {shiphero_line_item_id} for Line Item ID: {line_item_id}...")
            line_items_table.update(line_item_id, {"shiphero_id": shiphero_line_item_id})
            print(f"Updated Airtable with ShipHero Line Item ID: {shiphero_line_item_id} for Line Item ID: {line_item_id}.")

def push_enqueued_pos_to_shiphero(purchase_orders):
    """Push enqueued purchase orders to ShipHero."""
    purchase_orders_table = Table(config.AIRTABLE_API_KEY, config.AIRTABLE_PRODUCTION_DEV_BASE_ID, "Purchase Orders")
    line_items_table = Table(config.AIRTABLE_API_KEY, config.AIRTABLE_PRODUCTION_DEV_BASE_ID, "Line Items")

    for po_record in purchase_orders:
        po_id = po_record['id']
        po_number = po_record['fields']['PO #']

        # Prepare the GraphQL query
        query = prepare_graphql_query(po_record)

        try:
            # Execute the GraphQL query
            response = execute_graphql_query(query)
            print(f"Successfully synced purchase order: {po_number} to ShipHero.")
            print(response)

            # Extract ShipHero Purchase Order ID and Line Item IDs
            shiphero_po_id = response['data']['purchase_order_create']['purchase_order']['id']
            shiphero_line_items = response['data']['purchase_order_create']['purchase_order']['line_items']['edges']

            # Update Airtable with ShipHero IDs
            update_airtable_with_shiphero_ids(purchase_orders_table, line_items_table, po_record, shiphero_po_id, shiphero_line_items)

            # Update Airtable status to "Synced"
            update_airtable_status(purchase_orders_table, po_id, "Synced")
        except requests.exceptions.RequestException as e:
            print(f"Failed to sync purchase order: {po_number} to ShipHero. Error: {e}")
            # Update Airtable status to "Failed"
            update_airtable_status(purchase_orders_table, po_id, "Failed")

def fetch_active_purchase_orders_from_shiphero():
    """Fetch active purchase orders from ShipHero."""
    query = {
        "query": f"""
        query {{
            purchase_orders(fulfillment_status: "Pending", warehouse_id: "{config.SHIPHERO_WAREHOUSE_ID}") {{
                complexity
                request_id
                data(last: 5) {{
                    edges {{
                        node {{
                            id
                            po_number
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
            }}
        }}
        """
    }
    response = execute_graphql_query(query)
    return response['data']['purchase_orders']['data']

def sync_purchase_order_data_from_shiphero():
    """Sync purchase order data from ShipHero to Airtable."""
    print("Fetching active purchase orders from ShipHero...")
    active_purchase_orders = fetch_active_purchase_orders_from_shiphero()
    print(f"Fetched {len(active_purchase_orders)} active purchase orders from ShipHero.")

    purchase_orders_table = Table(config.AIRTABLE_API_KEY, config.AIRTABLE_PRODUCTION_DEV_BASE_ID, "Purchase Orders")
    line_items_table = Table(config.AIRTABLE_API_KEY, config.AIRTABLE_PRODUCTION_DEV_BASE_ID, "Line Items")

    for po_edge in active_purchase_orders:
        ### NOTE TO SELF DEBUGGING HERE AS OF 3/2: Trying to get po graphql response to play nice being synced back to airtable
        po = po_edge['node']        
        print("Processing Purchase Order:", po)  # Debugging statement

        shiphero_po_id = po['id']
        fulfillment_status = po['fulfillment_status']
        po_number = po['po_number']

        # Find the corresponding PO in Airtable
        airtable_po = purchase_orders_table.first(formula=f"{{shiphero_id}} = '{shiphero_po_id}'")
        if airtable_po:
            po_id = airtable_po['id']
            print(f"Updating Airtable PO ID: {po_id} with fulfillment status: {fulfillment_status}...")
            purchase_orders_table.update(po_id, {"Status (ShipHero)": fulfillment_status})
            print(f"Updated Airtable PO ID: {po_id} with fulfillment status: {fulfillment_status}.")

            # Update line items
            for line_item in po['line_items']['data']:
                shiphero_line_item_id = line_item['id']
                quantity = line_item['quantity']
                quantity_received = line_item['quantity_received']
                sku = line_item['sku']

                # Find the corresponding line item in Airtable
                airtable_line_item = line_items_table.first(formula=f"{{shiphero_id}} = '{shiphero_line_item_id}'")
                if airtable_line_item:
                    line_item_id = airtable_line_item['id']
                    print(f"Updating Airtable Line Item ID: {line_item_id} with quantity: {quantity} and quantity received: {quantity_received}...")
                    line_items_table.update(line_item_id, {
                        "Order Quantity (ShipHero)": quantity,
                        "Quantity Received (ShipHero)": quantity_received
                    })
                    print(f"Updated Airtable Line Item ID: {line_item_id} with quantity: {quantity} and quantity received: {quantity_received}.")
        else:
            print(f"No matching Airtable PO found for ShipHero PO ID: {shiphero_po_id}")

def sync_shiphero():
    print("Syncing new purchase orders from Airtable to ShipHero...")
    purchase_orders = fetch_purchase_orders_to_sync()
    push_enqueued_pos_to_shiphero(purchase_orders)
    print("Finished syncing new purchase orders from Airtable to ShipHero.")
    # print("Syncing purchase order data from ShipHero to Airtable...")
    # sync_purchase_order_data_from_shiphero()
    # print("Finished syncing purchase order data from ShipHero to Airtable.")