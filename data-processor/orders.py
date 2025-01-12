import pandas as pd
import os
from datetime import datetime
from database_connection import db

def read_orders_csv(file_path):
    """
    Read and validate the orders CSV file
    
    Args:
        file_path (str): Path to the CSV file
        
    Returns:
        pd.DataFrame: DataFrame containing the orders data
    
    Raises:
        FileNotFoundError: If the CSV file doesn't exist
        ValueError: If required columns are missing
    """
    try:
        # Check if file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"CSV file not found at: {file_path}")
            
        # Read the CSV file
        df = pd.read_csv(file_path)
        
        # Required columns in the CSV
        required_columns = [
            'Order ID', 'Website', 'Order Date', 'Currency',
            'Order Status', 'Shipping Address', 'Billing Address',
            'Total Owed', 'Shipping Charge', 'Total Discounts',
            'ASIN', 'Quantity', 'Unit Price', 'Unit Price Tax',
            'Shipment Status', 'Ship Date', 'Shipping Option',
            'Carrier Name & Tracking Number'
        ]
        
        # Validate required columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
            
        # Print basic information about the data
        print("\nOrders CSV Summary:")
        print(f"- Total rows: {len(df)}")
        print(f"- Unique orders: {df['Order ID'].nunique()}")
        print(f"- Date range: {df['Order Date'].min()} to {df['Order Date'].max()}")
        print(f"- Unique products: {df['ASIN'].nunique()}")
        
        return df
        
    except pd.errors.EmptyDataError:
        print("Error: The CSV file is empty")
        raise
    except pd.errors.ParserError:
        print("Error: Unable to parse the CSV file. Please check the file format")
        raise
    except Exception as e:
        print(f"Error reading CSV file: {str(e)}")
        raise

def store_orders(df):
    """
    Store orders data into the database
    
    Args:
        df (pd.DataFrame): DataFrame containing the orders data
        
    Returns:
        tuple: (orders_processed, orders_failed)
    """
    orders_processed = 0
    orders_failed = 0
    
    try:
        with db.get_cursor() as cur:
            # Process each unique order
            for order_id in df['Order ID'].unique():
                try:
                    order_data = df[df['Order ID'] == order_id].iloc[0]
                    
                    # Debug logging for numeric fields
                    print(f"\nProcessing order {order_id}:")
                    print(f"- Total Owed: {order_data['Total Owed']} (type: {type(order_data['Total Owed'])})")
                    print(f"- Shipping Charge: {order_data['Shipping Charge']} (type: {type(order_data['Shipping Charge'])})")
                    print(f"- Total Discounts: {order_data['Total Discounts']} (type: {type(order_data['Total Discounts'])})")
                    
                    # Convert order date to timestamp
                    order_date = datetime.strptime(
                        order_data['Order Date'], 
                        '%Y-%m-%dT%H:%M:%SZ'
                    )
                    
                    # Safely convert numeric values
                    try:
                        total_owed = float(str(order_data['Total Owed']).replace('"', '').replace("'", ""))
                        shipping_charge = float(str(order_data['Shipping Charge']).replace('"', '').replace("'", ""))
                        total_discounts = (
                            float(str(order_data['Total Discounts']).replace('"', '').replace("'", ""))
                            if order_data['Total Discounts'] != 'Not Available' 
                            else 0
                        )
                    except ValueError as ve:
                        print(f"Error converting numeric values for order {order_id}:")
                        print(f"- Total Owed value: '{order_data['Total Owed']}'")
                        print(f"- Shipping Charge value: '{order_data['Shipping Charge']}'")
                        print(f"- Total Discounts value: '{order_data['Total Discounts']}'")
                        raise ValueError(f"Failed to convert numeric values: {str(ve)}") from ve
                    
                    # Insert order
                    cur.execute("""
                        INSERT INTO orders (
                            order_id, website, order_date, currency,
                            order_status, total_owed, shipping_charge,
                            total_discounts, created_at, updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (order_id) 
                        DO UPDATE SET
                            order_status = EXCLUDED.order_status,
                            total_owed = EXCLUDED.total_owed,
                            shipping_charge = EXCLUDED.shipping_charge,
                            total_discounts = EXCLUDED.total_discounts,
                            updated_at = CURRENT_TIMESTAMP
                        RETURNING id
                    """, (
                        order_id,
                        order_data['Website'],
                        order_date,
                        order_data['Currency'],
                        order_data['Order Status'],
                        total_owed,
                        shipping_charge,
                        total_discounts,
                        datetime.now(),
                        datetime.now()
                    ))
                    
                    orders_processed += 1
                    
                except Exception as e:
                    print(f"\nError processing order {order_id}:")
                    print(f"- Error type: {type(e).__name__}")
                    print(f"- Error message: {str(e)}")
                    print("- Order data:")
                    for key, value in order_data.items():
                        print(f"  {key}: '{value}' (type: {type(value)})")
                    orders_failed += 1
                    continue
        
        print("\nOrder Storage Summary:")
        print(f"- Orders processed successfully: {orders_processed}")
        print(f"- Orders failed: {orders_failed}")
        
        return orders_processed, orders_failed
        
    except Exception as e:
        print(f"Database error: {str(e)}")
        raise e from None

if __name__ == "__main__":
    csv_file = "/Users/pgangasani/Documents/development/ai-agents/ai-agent-amazon-orders/data/orders.csv"
    
    # Read orders data
    orders_df = read_orders_csv(csv_file)
    
    # Store orders in database
    processed, failed = store_orders(orders_df)
