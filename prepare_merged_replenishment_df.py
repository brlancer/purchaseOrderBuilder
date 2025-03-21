import pandas as pd
import os
from datetime import datetime
import re

def prepare_merged_replenishment_df(stock_levels_df, sales_df, product_metadata_df):
    # Rename columns to a standardized convention
    stock_levels_df.rename(columns={
        'SKU': 'sku',
        'On Hand': 'on_hand',
        'committed': 'committed',
        'Available': 'available',
        'Backorder': 'backorder',
        'Incoming Stock': 'incoming'
    }, inplace=True)

    product_metadata_df.rename(columns={
        'SKU': 'sku',
        'Option1 Value': 'option1_value',
        'Position': 'position',
        'Cost-Production: Total': 'cost_production_total',
        'Product Name': 'product_name',
        'Category': 'category',
        'Subcategory': 'subcategory',
        'Product Number': 'product_num',
        'Product Type (Internal)': 'product_type_internal',
        'Supplier (Plain Text)': 'supplier',
        'Status Shopify (Shopify)': 'status_shopify',
        'Stocked Status': 'stocked_status',
        'Decoration Group (Plain Text)': 'decoration_group',
        'Artwork (Title)': 'artwork_title',
        'Component Brand': 'component_brand',
        'Component Style Number': 'component_style_number',
        'Component Style Name': 'component_style_name',
        'Component Color': 'component_color',
        'Blank Preferred Supplier': 'blank_preferred_supplier',
        'Blank Backup Supplier(s)': 'blank_backup_suppliers'
    }, inplace=True)

    # Inner merge stock_levels_df and product_metadata_df on SKU
    replenishment_df = stock_levels_df.merge(product_metadata_df, on='sku', how='inner')

    # Left merge with sales_df on SKU, filling missing sales data with 0
    replenishment_df = replenishment_df.merge(sales_df, on='sku', how='left').fillna(0)

    # Fill missing values in specific columns with empty strings
    columns_to_fill = ['option1_value', 'product_name', 'category', 'subcategory', 
                       'supplier', 'status_shopify', 'decoration_group', 'artwork_title']
    replenishment_df[columns_to_fill] = replenishment_df[columns_to_fill].fillna('')

    # Convert columns to strings to avoid unhashable type errors
    for column in ['decoration_group', 'product_type_internal', 'product_num', 'position']:
        replenishment_df[column] = replenishment_df[column].apply(lambda x: str(x))

    # Sort replenishment_df by decoration_group, product_type_internal, product_num, and position
    replenishment_df.sort_values(by=['decoration_group', 'product_type_internal', 'product_num', 'position'], inplace=True)

    # Reorder columns: product_metadata columns, sales_df columns, stock_levels_df columns
    product_metadata_columns = ['sku', 'option1_value', 'cost_production_total', 'product_name', 'category', 'subcategory', 'product_num', 'product_type_internal', 'supplier', 'status_shopify', 'stocked_status', 'decoration_group', 'artwork_title']
    # Extract sales columns with the pattern 'sales_X_weeks_ago_YYYYMMDD'
    sales_columns = [col for col in sales_df.columns if re.match(r'sales_\d+_weeks_ago_\w+\d{2}', col)]
    print("Sales Columns:", sales_columns)  # Debugging statement
    # Sort sales columns by the week number
    sales_columns_sorted = sorted(sales_columns, key=lambda x: int(re.search(r'sales_(\d+)_weeks_ago', x).group(1)), reverse=True)
    print("Sorted Sales Columns:", sales_columns_sorted)  # Debugging statement
    
    stock_levels_columns = ['on_hand', 'committed', 'available', 'backorder', 'incoming']
    other_columns = [col for col in replenishment_df.columns if col not in product_metadata_columns + sales_columns_sorted + stock_levels_columns]
    ordered_columns = product_metadata_columns + sales_columns_sorted + stock_levels_columns + other_columns
    
    # Print ordered columns
    print("Ordered Columns:")
    print(ordered_columns)
    
    replenishment_df = replenishment_df[ordered_columns]

    # Remove position field
    replenishment_df.drop(columns=['position'], inplace=True)

    # Remove extra characters from all columns with type string
    for column in replenishment_df.select_dtypes(include='object').columns:
        replenishment_df[column] = replenishment_df[column].apply(
           lambda x: re.sub(r"[\[\]\'\"]", "", x) if isinstance(x, str) else x
        )
    print("After removing extra characters from string columns:")
    print(replenishment_df.head())

    # Add "Updated At" field with the current timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    replenishment_df['updated_at'] = timestamp

    return replenishment_df


    