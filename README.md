# Amazon Orders Data Processor

A Python application to process and store Amazon order data in a PostgreSQL database.

## Overview

This application processes Amazon order data from CSV files and stores it in a structured database. It handles:
- Products
- Orders
- Order Items
- Shipping/Billing Addresses

## Project Structure

- `data-processor/`: Contains the main application logic.
  - `config.py`: Configuration settings.
  - `orders.py`: Order processing logic.
  - `order_items.py`: Order item processing logic.
  - `addresses.py`: Address processing logic.
  - `products.py`: Product processing logic.
  - `csv_importer.py`: CSV import logic.

## Prerequisites

- Python 3.x
- PostgreSQL

## Installation

1. Create and activate a virtual environment (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

1. Clone the repository.
2. Set up your PostgreSQL database and create the necessary tables.
3. Configure the `config.py` file with your database connection details.
4. Run the `csv_importer.py` script to import the CSV data into the database.
5. Update the `orders.csv` file with the correct data.

## License

This project is open-sourced under the MIT License - see the LICENSE file for details.
