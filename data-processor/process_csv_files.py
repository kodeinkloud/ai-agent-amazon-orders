import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
from datetime import datetime
import logging
import re

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='data_import.log'
)

# Database connection parameters
DB_PARAMS = {
    "dbname": "<your-database-name>",
    "user": "<your-username>",
    "password": "<your-password>",
    "host": "<your-host>",
    "port": "<your-port>"
}

class AmazonDataImporter:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.conn = None
        self.cursor = None

    def connect_to_db(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**DB_PARAMS)
            self.cursor = self.conn.cursor()
            logging.info("Successfully connected to database")
        except Exception as e:
            logging.error(f"Error connecting to database: {e}")
            raise

    def close_connection(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed")

    def process_products(self, df):
        """Process and insert products data"""
        try:
            # Check if we're processing digital items or regular orders
            if 'ProductName' in df.columns:
                product_name_col = 'ProductName'
            elif 'Product Name' in df.columns:
                product_name_col = 'Product Name'
            else:
                raise ValueError("No product name column found in DataFrame")

            # Prepare products data
            products_data = df[['ASIN', product_name_col]].drop_duplicates(
                subset=['ASIN'], 
                keep='last'
            )
            
            logging.info(f"Processing {len(products_data)} unique products")
        
            # Insert products
            insert_query = """
                INSERT INTO products (asin, product_name)
                VALUES %s
                ON CONFLICT (asin) DO UPDATE 
                SET product_name = EXCLUDED.product_name,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id, asin
            """
            
            products_tuples = [(
                row['ASIN'],
                row[product_name_col] if pd.notna(row[product_name_col]) else None
            ) for _, row in products_data.iterrows()]
            
            execute_values(self.cursor, insert_query, products_tuples)
            self.conn.commit()
            logging.info(f"Inserted/updated {len(products_data)} products")
            
        except Exception as e:
            logging.error(f"Error processing products: {e}")
            self.conn.rollback()
            raise

    def process_orders(self, df):
        """Process and insert orders data"""
        try:
            # Process orders
            orders_data = df[[
                'Order ID', 'Website', 'Order Date', 'Currency',
                'Total Owed', 'Shipping Charge', 'Total Discounts'
            ]].drop_duplicates()

            # Insert orders
            insert_query = """
                INSERT INTO orders (
                    order_id, website, order_date, currency,
                    total_owed, shipping_charge, total_discounts
                )
                VALUES %s
                ON CONFLICT (order_id) DO NOTHING
                RETURNING id
            """
            
            def clean_monetary_value(value):
                """Clean monetary values by removing currency symbols and handling negative values"""
                try:
                    # Convert to string and remove currency symbols and commas
                    cleaned = str(value).replace('$', '').replace(',', '')
                    # Remove quotes if present
                    cleaned = cleaned.replace('"', '').replace("'", '')
                    return float(cleaned)
                except (ValueError, AttributeError):
                    logging.warning(f"Could not convert value: {value}, defaulting to 0.0")
                    return 0.0

            orders_tuples = [(
                row['Order ID'],
                row['Website'],
                pd.to_datetime(row['Order Date']),
                row['Currency'],
                clean_monetary_value(row['Total Owed']),
                clean_monetary_value(row['Shipping Charge']),
                clean_monetary_value(row['Total Discounts'])
            ) for _, row in orders_data.iterrows()]

            execute_values(self.cursor, insert_query, orders_tuples)
            self.conn.commit()
            logging.info(f"Inserted {len(orders_data)} orders")

        except Exception as e:
            logging.error(f"Error processing orders: {e}")
            self.conn.rollback()
            raise

    def process_order_items(self, df):
        """Process and insert order items"""
        try:
            # Process order items
            items_data = df[[
                'Order ID', 'ASIN', 'Quantity', 'Unit Price',
                'Unit Price Tax', 'Shipment Status', 'Ship Date'
            ]].drop_duplicates()

            # Insert order items
            insert_query = """
                INSERT INTO order_items (
                    order_id, product_id, quantity, unit_price,
                    unit_price_tax, shipment_status, ship_date
                )
                SELECT 
                    %s, p.id, %s, %s, %s, %s::shipment_status_enum, %s
                FROM products p
                WHERE p.asin = %s
                ON CONFLICT DO NOTHING
            """
            
            def parse_date(date_str):
                """Parse date string and handle 'Not Available' and ISO format"""
                try:
                    if pd.isna(date_str) or date_str == 'Not Available':
                        return None
                    # Handle ISO format with Z timezone
                    if isinstance(date_str, str) and 'Z' in date_str:
                        # Remove the Z and convert to UTC
                        date_str = date_str.replace('Z', '+00:00')
                    return pd.to_datetime(date_str, utc=True)
                except Exception as e:
                    logging.warning(f"Could not parse date: {date_str}. Error: {e}")
                    return None

            def map_shipment_status(status):
                """Map shipment status to valid enum values"""
                if pd.isna(status) or status == 'Not Available':
                    return 'Pending'
                status_map = {
                    'Shipped': 'Shipped',
                    'Delivered': 'Delivered',
                    'Pending': 'Pending'
                }
                return status_map.get(status, 'Pending')

            def clean_quantity(qty):
                """Ensure quantity is at least 1"""
                try:
                    qty = int(qty)
                    return max(1, qty)
                except (ValueError, TypeError):
                    return 1

            for _, row in items_data.iterrows():
                try:
                    self.cursor.execute(insert_query, (
                        row['Order ID'],
                        clean_quantity(row['Quantity']),
                        float(str(row['Unit Price']).replace('$', '').replace(',', '')),
                        float(str(row['Unit Price Tax']).replace('$', '').replace(',', '')),
                        map_shipment_status(row['Shipment Status']),
                        parse_date(row['Ship Date']),
                        row['ASIN']
                    ))
                except Exception as e:
                    logging.error(f"Error processing row: {row}. Error: {e}")
                    continue

            self.conn.commit()
            logging.info(f"Inserted {len(items_data)} order items")

        except Exception as e:
            logging.error(f"Error processing order items: {e}")
            self.conn.rollback()
            raise

    def process_digital_orders(self, df):
        """Process and insert digital orders data"""
        try:
            # Process digital orders
            digital_orders_data = df[[
                'OrderId', 'DeliveryPacketId', 'Marketplace', 'OrderDate',
                'DeliveryDate', 'DeliveryStatus', 'OrderStatus', 'BillingAddress',
                'CountryCode'
            ]].drop_duplicates(subset=['OrderId'])

            # Insert digital orders
            insert_query = """
                INSERT INTO digital_orders (
                    order_id, delivery_packet_id, marketplace, order_date,
                    fulfilled_date, is_fulfilled, currency
                )
                VALUES %s
                ON CONFLICT (order_id) DO UPDATE 
                SET 
                    fulfilled_date = EXCLUDED.fulfilled_date,
                    is_fulfilled = EXCLUDED.is_fulfilled
                RETURNING id, order_id
            """
            
            digital_orders_tuples = [(
                row['OrderId'],
                row['DeliveryPacketId'],
                row['Marketplace'],
                pd.to_datetime(row['OrderDate']),
                pd.to_datetime(row['DeliveryDate']) if pd.notna(row['DeliveryDate']) else None,
                row['DeliveryStatus'] == 'Delivery Complete',
                'USD'  # Default currency, adjust as needed
            ) for _, row in digital_orders_data.iterrows()]

            execute_values(self.cursor, insert_query, digital_orders_tuples)
            self.conn.commit()
            logging.info(f"Inserted/updated {len(digital_orders_data)} digital orders")

        except Exception as e:
            logging.error(f"Error processing digital orders: {e}")
            self.conn.rollback()
            raise

    def process_digital_items(self, df):
        """Process and insert digital order items"""
        try:
            # Process digital items
            items_data = df[[
                'OrderId', 'DigitalOrderItemId', 'ASIN', 'ProductName',
                'QuantityOrdered', 'OurPrice'
            ]].drop_duplicates()

            # First ensure products exist
            self.process_products(df[['ASIN', 'ProductName']].drop_duplicates())

            # Insert digital order items
            insert_query = """
                INSERT INTO digital_order_items (
                    digital_order_id, product_id, quantity, unit_price
                )
                SELECT 
                    do.id,
                    p.id,
                    %s,
                    %s
                FROM digital_orders do
                JOIN products p ON p.asin = %s
                WHERE do.order_id = %s
                ON CONFLICT DO NOTHING
            """

            for _, row in items_data.iterrows():
                try:
                    self.cursor.execute(insert_query, (
                        int(row['QuantityOrdered']) if pd.notna(row['QuantityOrdered']) else 1,
                        float(str(row['OurPrice']).replace('$', '').replace(',', '')) if pd.notna(row['OurPrice']) else 0.0,
                        row['ASIN'],
                        row['OrderId']
                    ))
                except Exception as e:
                    logging.warning(f"Error processing digital item: {row['DigitalOrderItemId']}. Error: {e}")
                    continue

            self.conn.commit()
            logging.info(f"Processed {len(items_data)} digital order items")

        except Exception as e:
            logging.error(f"Error processing digital items: {e}")
            self.conn.rollback()
            raise

    def process_digital_payments(self, df):
        """Process and insert digital order payments"""
        try:
            # Process payments data
            payments_data = df[[
                'DeliveryPacketId', 'TransactionAmount', 'BaseCurrencyCode',
                'ClaimCode', 'MonetaryComponentTypeCode', 'OfferTypeCode'
            ]].drop_duplicates()

            # Insert digital order payments
            insert_query = """
                INSERT INTO digital_order_payments (
                    digital_order_id, transaction_amount, currency,
                    claim_code, monetary_component_type, offer_type
                )
                SELECT 
                    do.id,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                FROM digital_orders do
                WHERE do.delivery_packet_id = %s
                ON CONFLICT DO NOTHING
            """

            def clean_monetary_value(value):
                """Clean monetary values by removing currency symbols and handling negative values"""
                try:
                    if pd.isna(value) or value == 'Not Available':
                        return 0.0
                    cleaned = str(value).replace('$', '').replace(',', '')
                    return float(cleaned)
                except (ValueError, AttributeError):
                    return 0.0

            for _, row in payments_data.iterrows():
                try:
                    self.cursor.execute(insert_query, (
                        clean_monetary_value(row['TransactionAmount']),
                        row['BaseCurrencyCode'] if row['BaseCurrencyCode'] != 'Not Available' else 'USD',
                        row['ClaimCode'] if row['ClaimCode'] != 'Not Available' else None,
                        row['MonetaryComponentTypeCode'],
                        row['OfferTypeCode'] if row['OfferTypeCode'] != 'Not Available' else None,
                        row['DeliveryPacketId']
                    ))
                except Exception as e:
                    logging.warning(f"Error processing digital payment for packet: {row['DeliveryPacketId']}. Error: {e}")
                    continue

            self.conn.commit()
            logging.info(f"Processed {len(payments_data)} digital order payments")

        except Exception as e:
            logging.error(f"Error processing digital payments: {e}")
            self.conn.rollback()
            raise

    def import_digital_orders(self, digital_orders_path, digital_items_path, digital_payments_path):
        """Import all digital orders related data"""
        try:
            # Read CSV files
            digital_orders_df = pd.read_csv(digital_orders_path)
            digital_items_df = pd.read_csv(digital_items_path)
            digital_payments_df = pd.read_csv(digital_payments_path)

            # Process each type of data
            self.process_digital_orders(digital_orders_df)
            self.process_digital_items(digital_items_df)
            self.process_digital_payments(digital_payments_df)

            logging.info("Successfully imported all digital orders data")

        except Exception as e:
            logging.error(f"Error importing digital orders data: {e}")
            raise

    def process_file(self, file_path):
        """Process a single CSV file"""
        try:
            logging.info(f"Processing file: {file_path}")
            df = pd.read_csv(file_path)
            
            # Process based on file type
            if 'OrderHistory' in file_path:
                self.process_products(df)
                self.process_orders(df)
                self.process_order_items(df)
            # Add more file type processing as needed
            
        except Exception as e:
            logging.error(f"Error processing file {file_path}: {e}")
            raise

    def import_data(self):
        """Main method to import all data"""
        try:
            # Connect to database
            self.connect_to_db()

            # Get all subdirectories in the base directory
            subdirs = [d for d in os.listdir(self.base_dir) 
                      if os.path.isdir(os.path.join(self.base_dir, d))]

            # Process Digital Orders (folders starting with 'Digital-Ordering')
            digital_orders_dirs = [d for d in subdirs if d.startswith('Digital-Ordering')]
            for dir_name in digital_orders_dirs:
                dir_path = os.path.join(self.base_dir, dir_name)
                digital_orders_path = os.path.join(dir_path, 'Digital Orders.csv')
                digital_items_path = os.path.join(dir_path, 'Digital Items.csv')
                digital_payments_path = os.path.join(dir_path, 'Digital Orders Monetary.csv')
                
                if all(os.path.exists(p) for p in [digital_orders_path, digital_items_path, digital_payments_path]):
                    logging.info(f"Processing digital orders from {dir_name}")
                    self.import_digital_orders(digital_orders_path, digital_items_path, digital_payments_path)
                else:
                    logging.warning(f"Some digital orders files are missing in {dir_name}")

            # Process Digital Borrows (folders starting with 'Digital.Borrows')
            digital_borrows_dirs = [d for d in subdirs if d.startswith('Digital.Borrows')]
            for dir_name in digital_borrows_dirs:
                dir_path = os.path.join(self.base_dir, dir_name)
                borrows_path = os.path.join(dir_path, f'{dir_name}.csv')
                
                if os.path.exists(borrows_path):
                    logging.info(f"Processing digital borrows from {dir_name}")
                    df = pd.read_csv(borrows_path)
                    self.process_digital_borrows(df)
                else:
                    logging.warning(f"Digital borrows file is missing in {dir_name}")

            # Process Retail Orders (folders starting with 'Retail.OrderHistory')
            retail_orders_dirs = [d for d in subdirs if d.startswith('Retail.OrderHistory')]
            for dir_name in retail_orders_dirs:
                dir_path = os.path.join(self.base_dir, dir_name)
                retail_orders_path = os.path.join(dir_path, f'{dir_name}.csv')
                
                if os.path.exists(retail_orders_path):
                    logging.info(f"Processing retail orders from {dir_name}")
                    df = pd.read_csv(retail_orders_path)
                    self.process_products(df)
                    self.process_orders(df)
                    self.process_order_items(df)
                else:
                    logging.warning(f"Retail orders file is missing in {dir_name}")

            # Process Returns and Refunds (folders starting with 'Retail.OrdersReturned')
            returns_dirs = [d for d in subdirs if d.startswith('Retail.OrdersReturned')]
            for dir_name in returns_dirs:
                dir_path = os.path.join(self.base_dir, dir_name)
                returns_path = os.path.join(dir_path, f'{dir_name}.csv')
                
                if os.path.exists(returns_path):
                    logging.info(f"Processing returns and refunds from {dir_name}")
                    df = pd.read_csv(returns_path)
                    logging.info(f"DataFrame columns: {df.columns.tolist()}")
                    logging.info(f"Sample data:\n{df.head()}")
                    self.process_addresses(df)
                    self.process_returns(df)
                    self.process_refunds(df)
                else:
                    logging.warning(f"Returns file is missing in {dir_name}")

            logging.info("Data import completed successfully")

        except Exception as e:
            logging.error(f"Error during data import: {e}")
            raise
        finally:
            self.close_connection()

    def process_digital_borrows(self, df):
        """Process and insert digital borrows data"""
        try:
            # Process digital borrows
            borrows_data = df[[
                'ASIN', 'LoanCreationDate', 'LoanAcceptanceDate', 
                'LoanStatus', 'LoanProgram', 'EndDate', 
                'DeliveryDeviceName', 'ContentType', 'IsFirstContentLoan'
            ]].drop_duplicates()

            # First ensure products exist
            self.process_products(df[['ASIN', 'ProductName']].drop_duplicates())

            # Insert digital borrows
            insert_query = """
                INSERT INTO digital_borrows (
                    asin, loan_creation_date, loan_acceptance_date,
                    loan_status, loan_program, end_date,
                    delivery_device_name, content_type, is_first_content_loan
                )
                VALUES %s
                ON CONFLICT DO NOTHING
            """

            def parse_date(date_str):
                """Parse date string and handle 'Not Available'"""
                try:
                    if pd.isna(date_str) or date_str == 'Not Available':
                        return None
                    return pd.to_datetime(date_str)
                except Exception:
                    return None

            def parse_boolean(value):
                """Convert 'Yes'/'No' to boolean"""
                if isinstance(value, str):
                    return value.lower() == 'yes'
                return False

            borrows_tuples = [(
                row['ASIN'],
                parse_date(row['LoanCreationDate']),
                parse_date(row['LoanAcceptanceDate']),
                row['LoanStatus'] if row['LoanStatus'] != 'Not Available' else None,
                row['LoanProgram'] if row['LoanProgram'] != 'Not Available' else None,
                parse_date(row['EndDate']),
                row['DeliveryDeviceName'] if row['DeliveryDeviceName'] != 'Not Available' else None,
                row['ContentType'] if row['ContentType'] != 'Not Available' else None,
                parse_boolean(row['IsFirstContentLoan'])
            ) for _, row in borrows_data.iterrows()]

            execute_values(self.cursor, insert_query, borrows_tuples)
            self.conn.commit()
            logging.info(f"Inserted {len(borrows_data)} digital borrows")

        except Exception as e:
            logging.error(f"Error processing digital borrows: {e}")
            self.conn.rollback()
            raise

    def process_addresses(self, df):
        """Process and insert shipping/billing addresses"""
        try:
            # First, log available columns for debugging
            logging.info(f"Available columns in DataFrame: {df.columns.tolist()}")

            # Define possible column names for shipping and billing addresses
            shipping_columns = ['ShippingAddress', 'Shipping Address', 'Ship-Address', 'Ship Address']
            billing_columns = ['BillingAddress', 'Billing Address', 'Bill-Address', 'Bill Address']

            # Find the actual column names in the DataFrame
            shipping_col = next((col for col in shipping_columns if col in df.columns), None)
            billing_col = next((col for col in billing_columns if col in df.columns), None)

            if not shipping_col and not billing_col:
                logging.warning("No address columns found in the DataFrame")
                return

            # Extract unique addresses
            addresses = []
            
            if shipping_col:
                shipping_addresses = df[[shipping_col]].drop_duplicates()
                addresses.extend([(row[shipping_col], 'shipping') 
                                for _, row in shipping_addresses.iterrows() 
                                if pd.notna(row[shipping_col])])
            
            if billing_col:
                billing_addresses = df[[billing_col]].drop_duplicates()
                addresses.extend([(row[billing_col], 'billing') 
                                for _, row in billing_addresses.iterrows() 
                                if pd.notna(row[billing_col])])

            def parse_address(address_info):
                """Parse address string into components"""
                address, addr_type = address_info
                if pd.isna(address) or address == 'Not Available':
                    return None
                
                try:
                    # Remove any 'Shipping Address:' or 'Billing Address:' prefixes
                    address = re.sub(r'^(Shipping|Billing)\s+Address:\s*', '', address, flags=re.IGNORECASE)
                    
                    # Split address into parts
                    parts = [p.strip() for p in address.split(',')]
                    
                    if len(parts) >= 3:
                        address_line1 = parts[0]
                        address_line2 = parts[1] if len(parts) > 3 else None
                        city = parts[-2]
                        
                        # Handle state and zip code
                        state_zip = parts[-1].strip().split()
                        state = state_zip[0] if state_zip else None
                        postal_code = state_zip[1] if len(state_zip) > 1 else None
                        
                        return (
                            address_line1,
                            address_line2,
                            city,
                            state,
                            postal_code,
                            'US'  # Default country
                        )
                except Exception as e:
                    logging.warning(f"Error parsing address: {address}. Error: {e}")
                    return None
                
                return None

            # Insert addresses
            insert_query = """
                INSERT INTO addresses (
                    address_line1, address_line2, city, state, postal_code, country
                )
                VALUES %s
                ON CONFLICT (address_line1, city, state, postal_code) 
                DO UPDATE SET updated_at = CURRENT_TIMESTAMP
                RETURNING id, address_line1
            """
            
            # Process addresses
            address_tuples = [
                parsed for addr in addresses 
                if (parsed := parse_address(addr)) is not None
            ]
            
            if address_tuples:
                execute_values(self.cursor, insert_query, address_tuples)
                self.conn.commit()
                logging.info(f"Inserted/updated {len(address_tuples)} addresses")
            else:
                logging.warning("No valid addresses found to insert")

        except Exception as e:
            logging.error(f"Error processing addresses: {e}")
            self.conn.rollback()
            raise

    def process_returns(self, df):
        """Process and insert returns data"""
        try:
            # Log available columns
            logging.info(f"Available columns for returns: {df.columns.tolist()}")

            # Define column name mappings (possible variations of column names)
            column_mappings = {
                'OrderId': ['OrderId', 'Order ID', 'Order-ID', 'order_id'],
                'ReturnAuthorizationId': ['ReturnAuthorizationId', 'Return Authorization ID', 'Return-Auth-ID', 'return_authorization_id'],
                'ReturnDate': ['ReturnDate', 'Return Date', 'return_date'],
                'ReturnReason': ['ReturnReason', 'Return Reason', 'reason', 'return_reason'],
                'ReturnStatus': ['ReturnStatus', 'Return Status', 'Status', 'return_status'],
                'TrackingId': ['TrackingId', 'Tracking ID', 'tracking_id'],
                'ReturnShipOption': ['ReturnShipOption', 'Return Ship Option', 'ShipOption', 'ship_option']
            }

            # Find actual column names in the DataFrame
            actual_columns = {}
            for standard_name, possible_names in column_mappings.items():
                found_name = next((col for col in possible_names if col in df.columns), None)
                if found_name:
                    actual_columns[standard_name] = found_name

            # Check if we have the minimum required columns
            required_columns = ['OrderId', 'ReturnAuthorizationId', 'ReturnDate']
            missing_columns = [col for col in required_columns if col not in actual_columns]
            
            if missing_columns:
                logging.error(f"Missing required columns for returns: {missing_columns}")
                return

            # Create returns_data with available columns
            returns_data = df[[actual_columns[col] for col in actual_columns.keys()]].copy()
            
            # Rename columns to standard names
            returns_data.rename(columns={v: k for k, v in actual_columns.items()}, inplace=True)

            # Fill missing columns with default values
            if 'ReturnStatus' not in returns_data.columns:
                returns_data['ReturnStatus'] = 'Pending'
            if 'ReturnReason' not in returns_data.columns:
                returns_data['ReturnReason'] = None
            if 'TrackingId' not in returns_data.columns:
                returns_data['TrackingId'] = None
            if 'ReturnShipOption' not in returns_data.columns:
                returns_data['ReturnShipOption'] = None

            # Drop duplicates
            returns_data = returns_data.drop_duplicates(subset=['ReturnAuthorizationId'])

            # Insert returns
            insert_query = """
                INSERT INTO returns (
                    return_authorization_id, order_item_id, return_date,
                    return_status, return_reason, tracking_id, return_ship_option
                )
                SELECT 
                    %s,
                    oi.id,
                    %s,
                    %s::return_status_enum,
                    %s,
                    %s,
                    %s
                FROM order_items oi
                JOIN orders o ON o.order_id = oi.order_id
                WHERE o.order_id = %s
                ON CONFLICT (return_authorization_id) DO NOTHING
                RETURNING id
            """

            def map_return_status(status):
                """Map return status to enum values"""
                if pd.isna(status):
                    return 'Pending'
                
                status_map = {
                    'Completed': 'Completed',
                    'Complete': 'Completed',
                    'Pending': 'Pending',
                    'Rejected': 'Rejected',
                    'Returned': 'Completed',
                    'In Progress': 'Pending'
                }
                return status_map.get(str(status).strip(), 'Pending')

            for _, row in returns_data.iterrows():
                try:
                    self.cursor.execute(insert_query, (
                        row['ReturnAuthorizationId'],
                        pd.to_datetime(row['ReturnDate']),
                        map_return_status(row.get('ReturnStatus')),
                        row.get('ReturnReason'),
                        row.get('TrackingId') if pd.notna(row.get('TrackingId')) else None,
                        row.get('ReturnShipOption'),
                        row['OrderId']
                    ))
                except Exception as e:
                    logging.warning(f"Error processing return: {row['ReturnAuthorizationId']}. Error: {e}")
                    continue

            self.conn.commit()
            logging.info(f"Processed {len(returns_data)} returns")

        except Exception as e:
            logging.error(f"Error processing returns: {e}")
            self.conn.rollback()
            raise

    def process_refunds(self, df):
        """Process and insert refunds data"""
        try:
            # Log available columns
            logging.info(f"Available columns for refunds: {df.columns.tolist()}")

            # Define column name mappings
            column_mappings = {
                'ReturnAuthorizationId': ['ReturnAuthorizationId', 'Return Authorization ID', 'Return-Auth-ID', 'return_authorization_id'],
                'ReversalId': ['ReversalId', 'Reversal ID', 'Reversal-ID', 'reversal_id', 'RefundId', 'Refund ID'],
                'RefundAmount': ['RefundAmount', 'Refund Amount', 'Amount', 'refund_amount', 'Amount Refunded'],
                'RefundDate': ['RefundDate', 'Refund Date', 'Date', 'refund_date'],
                'RefundStatus': ['RefundStatus', 'Refund Status', 'Status', 'refund_status'],
                'Currency': ['Currency', 'currency', 'CurrencyCode']
            }

            # Find actual column names in the DataFrame
            actual_columns = {}
            for standard_name, possible_names in column_mappings.items():
                found_name = next((col for col in possible_names if col in df.columns), None)
                if found_name:
                    actual_columns[standard_name] = found_name
                else:
                    logging.warning(f"Column not found for {standard_name}. Available columns: {df.columns.tolist()}")

            # Check if we have the minimum required columns
            required_columns = ['ReturnAuthorizationId', 'RefundAmount']
            missing_columns = [col for col in required_columns if col not in actual_columns]
            
            if missing_columns:
                logging.error(f"Missing required columns for refunds: {missing_columns}")
                return

            # Select only available columns
            available_columns = [actual_columns[col] for col in actual_columns.keys()]
            refunds_data = df[available_columns].copy()
            
            # Rename columns to standard names
            refunds_data.rename(columns={v: k for k, v in actual_columns.items()}, inplace=True)

            # Fill missing columns with default values
            if 'ReversalId' not in refunds_data.columns:
                refunds_data['ReversalId'] = refunds_data['ReturnAuthorizationId'].apply(lambda x: f"REV-{x}")
            if 'RefundDate' not in refunds_data.columns:
                refunds_data['RefundDate'] = pd.Timestamp.now()
            if 'RefundStatus' not in refunds_data.columns:
                refunds_data['RefundStatus'] = 'Completed'
            if 'Currency' not in refunds_data.columns:
                refunds_data['Currency'] = 'USD'

            # Drop duplicates
            refunds_data = refunds_data.drop_duplicates(subset=['ReturnAuthorizationId'])

            # Insert refunds
            insert_query = """
                INSERT INTO refunds (
                    return_id, reversal_id, amount_refunded,
                    refund_date, status, currency
                )
                SELECT 
                    r.id,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                FROM returns r
                WHERE r.return_authorization_id = %s
                ON CONFLICT (reversal_id) DO NOTHING
            """

            def clean_monetary_value(value):
                """Clean monetary values"""
                try:
                    if pd.isna(value) or value == 'Not Available':
                        return 0.0
                    # Remove currency symbols and commas
                    cleaned = str(value).replace('$', '').replace(',', '').strip()
                    return float(cleaned)
                except (ValueError, AttributeError) as e:
                    logging.warning(f"Error cleaning monetary value {value}: {e}")
                    return 0.0

            successful_inserts = 0
            for _, row in refunds_data.iterrows():
                try:
                    self.cursor.execute(insert_query, (
                        row['ReversalId'],
                        clean_monetary_value(row['RefundAmount']),
                        pd.to_datetime(row['RefundDate']) if pd.notna(row.get('RefundDate')) else pd.Timestamp.now(),
                        row.get('RefundStatus', 'Completed'),
                        row.get('Currency', 'USD'),
                        row['ReturnAuthorizationId']
                    ))
                    if self.cursor.rowcount > 0:
                        successful_inserts += 1
                except Exception as e:
                    logging.warning(f"Error processing refund for return {row['ReturnAuthorizationId']}: {e}")
                    continue

            self.conn.commit()
            logging.info(f"Successfully processed {successful_inserts} refunds out of {len(refunds_data)} records")

        except Exception as e:
            logging.error(f"Error processing refunds: {e}")
            self.conn.rollback()
            raise

def main():
    base_dir = '<your-base-directory>'
    importer = AmazonDataImporter(base_dir)
    importer.import_data()

if __name__ == "__main__":
    main()