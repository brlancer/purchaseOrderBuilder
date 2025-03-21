import os
import sys
from pyairtable import Api
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import config
import json
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table as PlatypusTable, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph

def fetch_purchase_orders_to_generate():
    
    # Initialize Airtable API client
    api = Api(config.AIRTABLE_API_KEY)
    
    # Initialize Airtable client
    purchase_orders_table = api.table(config.AIRTABLE_PRODUCTION_DEV_BASE_ID, "Purchase Orders")
    line_items_table = api.table(config.AIRTABLE_PRODUCTION_DEV_BASE_ID, "Line Items")
    
    # Fetch purchase orders with the formula field "Generate packing slips?" set to True
    purchase_orders = purchase_orders_table.all(formula="{Generate packing slip?}", fields = ["PO #", "Supplier Name", "Shipping Address", "Ship Date"])

    # If no purchase orders are found, return early
    if not purchase_orders:
        print("No purchase orders selected for packing slip generation.")
        return

    # Fetch line items for each purchase order
    for po_record in purchase_orders:
        po_number = po_record['fields']['PO #']
        print(f"Fetching line items for purchase order number: {po_number}...")
        line_items = line_items_table.all(formula=f"{{PO #}} = '{po_number}'", fields=['Position',  'Line Item Name', 'sku', 'Quantity Ordered', 'Quantity Received'])
        # Sort line items by 'Position'
        line_items.sort(key=lambda item: item['fields']['Position'])
        print(f"Fetched {len(line_items)} line items for purchase order number: {po_number}.")
        po_record['line_items'] = line_items

    # Print the contents of the first purchase order for debugging purposes
    # print(json.dumps(purchase_orders[0], indent=2))

    return purchase_orders

def generate_packing_slip(order):
    filename = f"packing_slip_{order['id']}.pdf"
    doc = SimpleDocTemplate(filename, pagesize=letter)
    elements = []

    # Title
    title_data = [[Paragraph("<b><font size=28>CULK</font></b>")], [Paragraph("<br/><br/><br/><font size=28>Packing Slip</font>")]]
    title_table = PlatypusTable(title_data, colWidths=[6.5 * inch], hAlign='CENTER')
    title_table.setStyle(TableStyle([
      ('FONTSIZE', (0, 0), (-1, -1), 24),
      ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
    ]))
    elements.append(title_table)
    elements.append(PlatypusTable([[""]], colWidths=[6.5 * inch]))  # Empty row for spacing
    elements.append(PlatypusTable([[""]], colWidths=[6.5 * inch]))  # Empty row for spacing

    # Header Fields
    header_data = [
        ["PO #:", order['fields']['PO #']],
        ["Vendor:", order['fields']['Supplier Name'][0]],
        ["Ship Date:", order['fields']['Ship Date']],
        ["Shipping Address:", order['fields']['Shipping Address']]
    ]
    header_table = PlatypusTable(header_data, colWidths=[1.5 * inch, 5 * inch])
    header_table.setStyle(TableStyle([
      ('BACKGROUND', (0, 0), (-1, -1), colors.white),
      ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
      ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
      ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
      ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
      ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ]))
    elements.append(header_table)
    elements.append(PlatypusTable([[""]], colWidths=[6.5 * inch]))  # Empty row for spacing

    # Line Items Table
    styles = getSampleStyleSheet()
    line_items_data = [["Line Item Name", "SKU", "Quantity"]]
    total_quantity = 0
    for item in order['line_items']:
      line_item_name = Paragraph(item['fields']['Line Item Name'][0], styles['Normal'])
      sku = item['fields']['sku'][0]
      quantity_ordered = item['fields']['Quantity Ordered']
      total_quantity += quantity_ordered
      line_items_data.append([line_item_name, sku, quantity_ordered])
    
    # Add total quantity as the last row
    line_items_data.append(["", "Total:", total_quantity])
    
    line_items_table = PlatypusTable(line_items_data, colWidths=[4.5 * inch, 1 * inch, 1 * inch])
    line_items_table.setStyle(TableStyle([
      # ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
      # ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
      ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
      ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
      ('BACKGROUND', (0, 1), (-1, -1), colors.white),
      ('GRID', (0, 1), (-1, -2), 1, colors.black),  # Apply grid only up to the second-to-last row
      ('VALIGN', (0, 0), (-1, -1), 'TOP'),
      ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),  # Bold for "Total Quantity"
      ('ALIGN', (0, -1), (-2, -1), 'RIGHT'),  # Align "Total" row to the right
      ('LINEBELOW', (0, -1), (-1, -1), 0, colors.white),  # Remove bottom border for "Total"
      ('LINEBEFORE', (0, -1), (-1, -1), 0, colors.white),  # Remove left border for "Total"
      ('TOPPADDING', (0, -1), (-1, -1), 6),  # Add top padding for the last row
    ]))
    elements.append(line_items_table)

    # Build the PDF
    doc.build(elements)

    # Save the PDF to the output directory for debugging purposes
    output_dir = "output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    debug_filename = os.path.join(output_dir, filename)
    os.rename(filename, debug_filename)
    print(f"Packing slip saved to {debug_filename} for debugging purposes.")

    return debug_filename

def upload_packing_slip(order, filename):
    
    # Initialize Airtable API client
    api = Api(config.AIRTABLE_API_KEY)

    # Initialize Airtable client
    purchase_orders_table = api.table(config.AIRTABLE_PRODUCTION_DEV_BASE_ID, "Purchase Orders")

    # Remove any existing attachments in the 'Packing slip' field and set the 'Generate packing slip?' field to False
    purchase_orders_table.update(order['id'], {"Packing slip": [], "Generate packing slip?": False})

    # Upload the packing slip as an attachment to the purchase order record
    with open(filename, 'rb') as file:
        purchase_orders_table.upload_attachment(order['id'], 'Packing slip', filename)

    print(f"Packing slip uploaded to purchase order number: {order['fields']['PO #']}.")


# Main function to generate and upload packing slips
def packing_slips():
    orders = fetch_purchase_orders_to_generate()
    for order in orders:
        filename = generate_packing_slip(order)
        upload_packing_slip(order, filename)
        os.remove(filename)