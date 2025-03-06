# prepare_replenishment.py
from fetch_data import fetch_shiphero_stock_levels, fetch_airtable_incoming_stock, fetch_shopify_sales_data, fetch_airtable_product_metadata, fetch_shopify_inventory_data
from transform_data import transform_stock_levels, transform_sales_data, transform_product_metadata
from prepare_merged_replenishment_df import prepare_merged_replenishment_df
from export_sheets_replenishment import export_sheets_replenishment

def prepare_replenishment(use_cache_stock_levels=False, use_cache_sales=False):
    
    # Prepare stock levels
    stock_levels_data = fetch_shiphero_stock_levels(use_cache=use_cache_stock_levels)
    incoming_stock_data = fetch_airtable_incoming_stock()
    committed_stock_data = fetch_shopify_inventory_data()
    stock_levels_df = transform_stock_levels(stock_levels_data, incoming_stock_data, committed_stock_data)

    # Prepare sales and product metadata
    sales_data = fetch_shopify_sales_data(use_cache=use_cache_sales)
    sales_df = transform_sales_data(sales_data)

    # Prepare product metadata
    product_metadata = fetch_airtable_product_metadata()
    product_metadata_df = transform_product_metadata(product_metadata)

    # Prepare merged replenishment DataFrame and export to Google Sheets
    replenishment_df = prepare_merged_replenishment_df(stock_levels_df, sales_df, product_metadata_df)
    export_sheets_replenishment(replenishment_df)