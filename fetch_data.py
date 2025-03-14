import requests, time, json, os, pickle
from urllib.parse import urlencode
from config import AIRTABLE_API_KEY, AIRTABLE_VARIANTS_ENDPOINT, AIRTABLE_PRODUCTION_DEV_BASE_ID, SHIPHERO_WAREHOUSE_ID
import pandas as pd
from pyairtable import Table
from datetime import datetime, timedelta
from utils import fetch_shiphero_paginated_data, fetch_shopify_bulk_operation


# Airtable functions

def fetch_airtable_incoming_stock():
    """
    Fetches incoming stock data from Airtable and processes it into a pandas DataFrame.
    This function retrieves records from the "Line Items" table in Airtable where the 
    "PO Status" is either 'Open' or 'Draft'. It extracts relevant fields from these records, 
    calculates the incoming stock by subtracting the received quantity from the ordered quantity, 
    and groups the data by SKU to sum the incoming stock for each SKU.
    Returns:
      pandas.DataFrame: A DataFrame containing the SKU and the summed incoming stock for each SKU.
    """

    print("Fetching incoming stock data from Airtable...")
    
    # print("Initializing Airtable table...")
    line_items_table = Table(AIRTABLE_API_KEY, AIRTABLE_PRODUCTION_DEV_BASE_ID, "Line Items")

    # print("Fetching records with PO Status = 'Open'...")
    records = line_items_table.all(formula="OR({PO Status} = 'Open', {PO Status} = 'Draft')", fields=['Position - PO # - SKU', 'sku', 'Quantity Ordered', 'Quantity Received'])
    # print(f"Fetched {len(records)} records.")
    # print("First 5 records:")
    # for record in records[:5]:
    #     print(record)

    # print("Extracting relevant fields...")
    data = []
    for record in records:
        fields = record['fields']
        data.append({
            'Position - PO # - SKU': fields.get('Position - PO # - SKU', ''),
            'sku': fields.get('sku', ''),
            'ordered': fields.get('Quantity Ordered', 0),
            'received': fields.get('Quantity Received', 0)
        })
    # print(f"Extracted data for {len(data)} records.")

    # print("Converting data to DataFrame...")
    df = pd.DataFrame(data)
    # print("DataFrame created:")
    # print(df.head())

    # print("Ensuring 'sku' column contains only strings and cleaning 'sku' values...")
    df['sku'] = df['sku'].apply(lambda x: str(x[0]) if isinstance(x, list) and len(x) > 0 else str(x) if not isinstance(x, str) else x)

    # print("Calculating 'pending' field...")
    df['incoming'] = df['ordered'] - df['received']
    # print("Calculated 'pending' field:")
    # print(df.head())

    # print("Grouping by 'sku' and summing 'incoming'...")
    grouped_df = df.groupby('sku')['incoming'].sum().reset_index()
    # print("Grouped DataFrame:")
    # print(grouped_df.head())

    return grouped_df

def fetch_airtable_product_metadata():
    """
    Fetches product metadata from Airtable and processes it into a pandas DataFrame.
    This function retrieves records from the "Variants" table in Airtable and extracts
    relevant fields from these records. It then converts the data into a DataFrame.
    Returns:
      pandas.DataFrame: A DataFrame containing the relevant product metadata fields.
    """

    headers = {
        'Authorization': f'Bearer {AIRTABLE_API_KEY}'
    }
    params = {
        'view': 'Data for PO Builder',
        'fields[]': [
            'SKU', 
            'product_num',
            'Product Name', 
            'Option1 Value', 
            'Position',
            'Supplier Name - ShipHero', 
            'Status Shopify (Shopify)',
            'Stocked Status',
            'Decoration Group (Plain Text)',
            'Artwork (Title)',
            'Cost-Production: Total', 
            'Category', 
            'Subcategory', 
            'Product Type (Internal)'
        ]
    }
    
    all_records = []
    offset = None

    while True:
        if offset:
            params['offset'] = offset
        encoded_params = urlencode(params, doseq=True)
        url = f"{AIRTABLE_VARIANTS_ENDPOINT}?{encoded_params}"
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            records = data.get('records', [])
            all_records.extend([record['fields'] for record in records])
            offset = data.get('offset')
            if not offset:
                break
        else:
            print(f"Failed to fetch data: {response.status_code}")
            print("Response Content:", response.content)
            return None

    return all_records


# Shiphero functions

def fetch_shiphero_stock_levels(use_cache=False):
    """
    Fetches stock levels data from ShipHero and processes it into a list of dictionaries.
    This function retrieves stock levels data from the ShipHero GraphQL API and paginates
    through the results to fetch all available data. It then processes the data into a list
    of dictionaries, where each dictionary represents a product and contains relevant fields.
    Returns:
      list: A list of dictionaries containing the stock levels data for each product.
    """
    
    CACHE_FILE = 'cache/shiphero_stock_levels.pkl'

    if use_cache and os.path.exists(CACHE_FILE):
        print("Loading cached stock levels data...")
        with open(CACHE_FILE, 'rb') as f:
          return pickle.load(f)
        
    print("Fetching fresh stock levels data from ShipHero...")

    query = """
    query ($first: Int!, $after: String) {
      warehouse_products(warehouse_id: "V2FyZWhvdXNlOjEwMTU4Mw==", active: true) { 
        complexity 
        request_id 
        data(first: $first, after: $after) { 
          pageInfo {
            hasNextPage
            endCursor
          }
          edges { 
            node { 
              id 
              sku
              on_hand
              allocated
              available
              backorder
            }
          }
        }
      }
    }
    """
    
    variables = {
        "first": 100,
        "after": None
    }
    
    stock_levels = fetch_shiphero_paginated_data(query, variables, "warehouse_products")

    # Save the fetched data to cache
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    with open(CACHE_FILE, 'wb') as f:
      pickle.dump(stock_levels, f)
        
    return stock_levels

def fetch_purchase_orders_from_shiphero(created_from: str = None):
  """Fetch active purchase orders from ShipHero."""
  
  if not created_from:
    raise ValueError("The 'created_from' parameter is required.")
  
  # Convert created_from to ISODateTime format
  try:
    created_from_iso = datetime.strptime(created_from, "%Y-%m-%d").isoformat()
    created_from = created_from_iso + "Z"
  except ValueError as e:
    raise ValueError(f"Invalid date format for 'created_from': {created_from}. Expected format: YYYY-MM-DD") from e

  query = """
  query ($first: Int!, $after: String, $created_from: ISODateTime, $warehouse_id: String){
    purchase_orders(created_from: $created_from, warehouse_id: $warehouse_id) {
      complexity
      request_id
      data(first: $first, after: $after) {
        pageInfo {
          hasNextPage
          endCursor
        }
        edges {
          node {
            id
            po_number
            fulfillment_status
            line_items {
              edges {
                node {
                  id
                  sku
                  quantity
                  quantity_received
                }
              }
            }
          }
        }
      }
    }
  }
  """
  
  variables = {
    "first": 10,
    "after": None,
    "created_from": created_from,
    "warehouse_id": SHIPHERO_WAREHOUSE_ID
  }

  # print the query and variables
  print(query)
  print(variables)
  purchase_orders = fetch_shiphero_paginated_data(query, variables, "purchase_orders")
  
  return purchase_orders

# Shopify functions

def fetch_shopify_sales_data(use_cache=False):
    """
    Fetches sales data from Shopify and processes it into a list of dictionaries.
    This function retrieves sales data from the Shopify GraphQL API and paginates
    through the results to fetch all available data. It then processes the data into
    a list of dictionaries, where each dictionary represents an order or a line item
    within an order and contains relevant fields.
    Returns:
      list: A list of dictionaries containing the sales data for each order and line item.
    """
    
    CACHE_FILE = 'cache/shopify_sales_data.pkl'

    if use_cache and os.path.exists(CACHE_FILE):
        print("Loading cached sales data...")
        with open(CACHE_FILE, 'rb') as f:
          return pickle.load(f)
        
    print("Fetching fresh sales data from Shopify...")

    # Calculate the date 9 weeks before today
    nine_weeks_ago = datetime.now() - timedelta(weeks=9)
    formatted_date = nine_weeks_ago.strftime("%Y-%m-%d")
    
    inner_query = f"""
    {{
      orders(query: "created_at:>={formatted_date} AND (fulfillment_status:shipped OR fulfillment_status:unfulfilled OR fulfillment_status:partial) AND (financial_status:paid OR financial_status:pending) AND -tag:'Exclude from Forecast'") {{
        edges {{
          node {{
            id
            name
            createdAt
            tags
            displayFulfillmentStatus
            displayFinancialStatus
            cancelledAt
            lineItems(first: 100) {{
              edges {{
                node {{
                  id
                  sku
                  variantTitle
                  quantity
                  unfulfilledQuantity
                }}
              }}
            }}
          }}
        }}
      }}
    }}
    """
    
    sales_data = fetch_shopify_bulk_operation(inner_query)

    # Save the fetched data to cache
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    with open(CACHE_FILE, 'wb') as f:
      pickle.dump(sales_data, f)

    return sales_data

def fetch_shopify_inventory_data(use_cache=False):
    """
    Fetches inventory data from Shopify and processes it into a pandas DataFrame.
    This function retrieves inventory data from the Shopify GraphQL API and processes
    it into a pandas DataFrame. It fetches data for products and their variants, including
    inventory levels at different locations. It then processes the data into a DataFrame
    with columns for product ID, product title, variant ID, variant title, SKU, location ID,
    location name, and inventory quantities (available, incoming, committed, on hand).
    Returns:
      pandas.DataFrame: A DataFrame containing the inventory data for products and variants.
    """
    
    CACHE_FILE = 'cache/shopify_inventory_data.pkl'

    if use_cache and os.path.exists(CACHE_FILE):
        print("Loading cached inventory data...")
        with open(CACHE_FILE, 'rb') as f:
          return pickle.load(f)
        
    print("Fetching fresh inventory data from Shopify...")

    query = """
    query GetCommittedInventory {
      products(first:50, query: "status:ACTIVE") {
        edges {
          node {
            id
            title
            variants(first:50) {
              edges {
                node {
                  id
                  title
                  sku
                  inventoryItem {
                    id
                    inventoryLevels(first: 10) {
                      edges {
                        node {
                          location {
                            id
                            name
                          }
                          quantities(names: ["available","incoming","committed","on_hand"]) {
                            name
                            quantity
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    """
    
    inventory_data = fetch_shopify_bulk_operation(query)

    # Save the fetched data to cache
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    with open(CACHE_FILE, 'wb') as f:
      pickle.dump(inventory_data, f)

    return inventory_data