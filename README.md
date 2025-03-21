# Culk Analytics

A Flask-based web application for preparing replenishment reports, sales analytics, and managing purchase orders.

## Overview

This tool performs several operations related to inventory management and purchase orders through a web interface.

## Requirements

- Python 3.x
- Dependencies (install via `pip install -r requirements.txt`)

## Usage

The application is run locally as a Flask web server. To start the server, execute:

```bash
python app.py
```

Once the server is running, open your web browser and navigate to `http://localhost:5000` to access the web interface.

## Functions

The web interface provides buttons to trigger the following functions:

1. Prepare Replenishment (Full Reload):

- Gathers current stock levels from ShipHero
- Retrieves past 8 weeks of sales data from Shopify
- Transforms sales data into time series
- Fetches product metadata from Airtable
- Uploads the dataset to the Replenishment worksheet in Google Drive

2. Prepare Replenishment (Use Cache):

- Uses cached stock levels and sales data to perform the same operations as the full reload

3. Populate Production:

- Retrieves reorder quantities from the Replenishment worksheet
- Transforms them into new Purchase Orders
- Populates the Production base in Airtable

4. Push POs to ShipHero:

- Pushes the newly created Purchase Orders to ShipHero

5. Syncs purchase orders from ShipHero to Airtable

- Optionally filters by creation date

## Examples

To start the Flask applicaiton:

```bash
python app.py
```

Navigate to http://localhost:5001 in your web browser to access the web interface and trigger the desired functions.

## Notes

Ensure all required services (ShipHero, Shopify, Airtable, Google Drive) are properly configured and accessible for the application to function correctly.
