import requests, os, time, json
from datetime import datetime, timedelta
from config import SHIPHERO_API_TOKEN, SHIPHERO_REFRESH_TOKEN, SHIPHERO_REFRESH_ENDPOINT, SHIPHERO_GRAPHQL_ENDPOINT, SHIPHERO_TOKEN_EXPIRATION
from config import SHOPIFY_API_TOKEN, SHOPIFY_GRAPHQL_ENDPOINT

# General purpose

def export_df(df, label):
    timestamp = datetime.now().strftime("%Y-%d-%m %H-%M-%S")
    output_dir = "output"
    output_path = os.path.join(output_dir, f"{label}_{timestamp}.csv")
    
    # Check if the directory exists and create it if not
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    print(f"{label}:")
    print(df)
    
    df.to_csv(output_path, index=False)
    print(f"{label} saved to {output_path}")

def export_json(data, label):
    timestamp = datetime.now().strftime("%Y-%d-%m %H-%M-%S")
    output_dir = "output"
    output_path = os.path.join(output_dir, f"{label}_{timestamp}.json")
    
    # Check if the directory exists and create it if not
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    print(f"{label}:")
    print(data)
    
    with open(output_path, 'w') as file:
        json.dump(data, file, indent=4)
    
    print(f"{label} saved to {output_path}")

# Shiphero API utility functions

def refresh_shiphero_token():
    headers = {
        "Content-Type": "application/json"
    }
    data = {
        "refresh_token": SHIPHERO_REFRESH_TOKEN
    }
    response = requests.post(SHIPHERO_REFRESH_ENDPOINT, json=data, headers=headers)
    if response.status_code == 200:
        response_data = response.json()
        new_token = response_data.get("access_token")
        expires_in = response_data.get("expires_in")  # Assuming the API returns the expiration time in seconds
        if new_token and expires_in:
            expiration_time = datetime.now() + timedelta(seconds=expires_in)
            print("ShipHero API token refreshed successfully.")
            update_config_file_with_new_shiphero_token(new_token, expiration_time)
            return new_token, expiration_time
    print("Failed to refresh ShipHero API token.")
    return None, None

def update_config_file_with_new_shiphero_token(new_token, expiration_time):
    config_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'config.py'))
    with open(config_file_path, 'r') as file:
        lines = file.readlines()
    
    with open(config_file_path, 'w') as file:
        for line in lines:
            if line.startswith('SHIPHERO_API_TOKEN'):
                file.write(f'SHIPHERO_API_TOKEN = "{new_token}"\n')
            elif line.startswith('SHIPHERO_TOKEN_EXPIRATION'):
                file.write(f'SHIPHERO_TOKEN_EXPIRATION = "{expiration_time.isoformat()}"\n')
            else:
                file.write(line)

def is_token_expired():
    expiration_time = datetime.fromisoformat(SHIPHERO_TOKEN_EXPIRATION)
    return datetime.now() >= expiration_time

def fetch_shiphero_with_throttling(query, variables):
    global SHIPHERO_API_TOKEN, SHIPHERO_TOKEN_EXPIRATION

    if is_token_expired():
        print("Token is expired. Refreshing token...")
        new_token, new_expiration = refresh_shiphero_token()
        if not new_token:
            raise Exception("Failed to refresh ShipHero API token.")
        SHIPHERO_API_TOKEN = new_token
        SHIPHERO_TOKEN_EXPIRATION = new_expiration.isoformat()
    
    headers = {
        "Authorization": f"Bearer {SHIPHERO_API_TOKEN}",
        "Content-Type": "application/json"
    }
    
    while True:
        response = requests.post(SHIPHERO_GRAPHQL_ENDPOINT, json={"query": query, "variables": variables}, headers=headers)
        
        if response.status_code == 200:
            result = response.json()
            
            if "errors" in result:
                error = result["errors"][0]
                if error["code"] == 30:
                    wait_time_str = error["time_remaining"]
                    wait_time = int(wait_time_str.split()[0])
                    print(f"Throttling detected. Waiting for {wait_time} seconds before retrying...")
                    time.sleep(wait_time)
                    continue
            return result
        else:
            print("Failed to fetch data")
            print(response.text)
            raise Exception("Failed to fetch data from ShipHero API")

def fetch_shiphero_paginated_data(query, variables, data_key):
    data_list = []
    has_next_page = True
    after_cursor = None

    while has_next_page:
        variables["after"] = after_cursor
        # print(f"Sending request with variables: {variables}")
        result = fetch_shiphero_with_throttling(query, variables)
        
        if result:
            data = result.get("data", {}).get(data_key, {}).get("data", {})
            if data and "edges" in data:
                data_list.extend(data["edges"])
                page_info = result.get("data", {}).get(data_key, {}).get("data", {}).get("pageInfo")
                if page_info:
                    has_next_page = page_info.get("hasNextPage", False)
                    after_cursor = page_info.get("endCursor")
                else:
                    print("No 'pageInfo' found in the response")
                    print("Response data:", result)
                    has_next_page = False
            else:
                print("No data found in the response or 'edges' key is missing")
                print("Response data:", result)
                has_next_page = False
        else:
            print("Failed to fetch data")
            has_next_page = False

    return data_list

# Shopify API utility functions

def start_bulk_operation(inner_query):
    mutation = f"""
    mutation {{
      bulkOperationRunQuery(
        query: \"\"\"
        {inner_query}
        \"\"\"
      ) {{
        bulkOperation {{
          id
          status
        }}
        userErrors {{
          field
          message
        }}
      }}
    }}
    """
    
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_API_TOKEN,
        "Content-Type": "application/json"
    }
    
    response = requests.post(SHOPIFY_GRAPHQL_ENDPOINT, json={"query": mutation}, headers=headers)
    
    if response.status_code == 200:
        result = response.json()
        print(response.text)
        return result
    else:
        print("Failed to start bulk operation")
        print(response.text)
        return None

def check_bulk_operation_status():
    query = """
    {
      currentBulkOperation {
        id
        status
        errorCode
        createdAt
        completedAt
        objectCount
        fileSize
        url
      }
    }
    """
    
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_API_TOKEN,
        "Content-Type": "application/json"
    }
    
    response = requests.post(SHOPIFY_GRAPHQL_ENDPOINT, json={"query": query}, headers=headers)
    
    if response.status_code == 200:
        result = response.json()
        return result
    else:
        print("Failed to check bulk operation status")
        print(response.text)
        return None

def download_bulk_operation_results(url):
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.text.splitlines()
        return [json.loads(line) for line in data]
    else:
        print("Failed to download bulk operation results")
        print(response.text)
        return None

def fetch_shopify_bulk_operation(inner_query):
    start_result = start_bulk_operation(inner_query)
    if not start_result:
        return None
    
    while True:
        status_result = check_bulk_operation_status()
        if not status_result:
            return None
        
        bulk_operation = status_result.get("data", {}).get("currentBulkOperation")
        if not bulk_operation:
            print("No current bulk operation found")
            return None
        
        status = bulk_operation.get("status")
        
        if status == "COMPLETED":
            print("Bulk operation completed")
            url = bulk_operation.get("url")
            return download_bulk_operation_results(url)
        elif status == "FAILED":
            print("Bulk operation failed")
            return None
        else:
            print(f"Bulk operation status: {status}")
            time.sleep(3)
