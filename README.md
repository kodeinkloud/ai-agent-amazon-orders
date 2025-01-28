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
  - `amazon_orders_agent.py`: SQL query agent.

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
4. Run the agent to query your order data.

## Example Queries

Here are some example questions you can ask the agent:

### Basic Order Statistics
- "How many orders are there?"
- "What's the average order value?"
  - Response: The average order value is approximately $26.01

### Monthly Analysis
- "List top 5 months with highest total spent"
  ```
  1. August 2018: $1,086.87
  2. August 2024: $1,081.69
  3. September 2018: $832.91
  4. June 2024: $778.91
  5. June 2021: $770.62
  ```

### Time-based Patterns
- "What day of the week has the most orders?"
  - Response: Saturday with 258 orders
- "What was the busiest month for orders?"
  - Response: November 2024 with 30 orders

## License

This project is open-sourced under the MIT License - see the LICENSE file for details.
