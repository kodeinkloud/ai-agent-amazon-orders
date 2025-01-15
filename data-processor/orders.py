import logging
import re
import pandas as pd
from config import ORDERS_CSV_FILE_PATH
from database_connection import db
from psycopg2.extras import execute_values

# Add this after the imports
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class OrdersImporter:
    def __init__(self, db_connection):
        self.db = db_connection
        self.db.connect_to_db()

    def import_orders_from_csv(self):
        logging.info(ORDERS_CSV_FILE_PATH)
        df = pd.read_csv(ORDERS_CSV_FILE_PATH)
        # self.process_products(df)
        # self.process_orders(df)
        # self.process_order_items(df)
        self.process_addresses(df)
    def process_products(self, df):
        """Process and insert products data"""
        try:
            # Check if we're processing digital items or regular orders
            if "ProductName" in df.columns:
                product_name_col = "ProductName"
            elif "Product Name" in df.columns:
                product_name_col = "Product Name"
            else:
                raise ValueError("No product name column found in DataFrame")

            # Prepare products data
            products_data = df[["ASIN", product_name_col]].drop_duplicates(
                subset=["ASIN"], keep="last"
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

            products_tuples = [
                (
                    row["ASIN"],
                    row[product_name_col] if pd.notna(row[product_name_col]) else None,
                )
                for _, row in products_data.iterrows()
            ]

            execute_values(self.db.cursor, insert_query, products_tuples)
            self.db.conn.commit()
            logging.info(f"Inserted/updated {len(products_data)} products")

        except Exception as e:
            logging.error(f"Error processing products: {e}")
            self.db.conn.rollback()
            raise

    def process_orders(self, df):
        """Process and insert orders data"""
        try:
            # Process orders
            orders_data = df[
                [
                    "Order ID",
                    "Website",
                    "Order Date",
                    "Currency",
                    "Total Owed",
                    "Shipping Charge",
                    "Total Discounts",
                ]
            ].drop_duplicates()

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
                    cleaned = str(value).replace("$", "").replace(",", "")
                    # Remove quotes if present
                    cleaned = cleaned.replace('"', "").replace("'", "")
                    return float(cleaned)
                except (ValueError, AttributeError):
                    logging.warning(
                        f"Could not convert value: {value}, defaulting to 0.0"
                    )
                    return 0.0

            orders_tuples = [
                (
                    row["Order ID"],
                    row["Website"],
                    pd.to_datetime(row["Order Date"]),
                    row["Currency"],
                    clean_monetary_value(row["Total Owed"]),
                    clean_monetary_value(row["Shipping Charge"]),
                    clean_monetary_value(row["Total Discounts"]),
                )
                for _, row in orders_data.iterrows()
            ]

            execute_values(self.db.cursor, insert_query, orders_tuples)
            self.db.conn.commit()
            logging.info(f"Inserted {len(orders_data)} orders")

        except Exception as e:
            logging.error(f"Error processing orders: {e}")
            self.db.conn.rollback()
            raise

    def process_order_items(self, df):
        """Process and insert order items"""
        try:
            # Process order items
            items_data = df[
                [
                    "Order ID",
                    "ASIN",
                    "Quantity",
                    "Unit Price",
                    "Unit Price Tax",
                    "Shipment Status",
                    "Ship Date",
                ]
            ].drop_duplicates()

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
                    if pd.isna(date_str) or date_str == "Not Available":
                        return None
                    # Handle ISO format with Z timezone
                    if isinstance(date_str, str) and "Z" in date_str:
                        # Remove the Z and convert to UTC
                        date_str = date_str.replace("Z", "+00:00")
                    return pd.to_datetime(date_str, utc=True)
                except Exception as e:
                    logging.warning(f"Could not parse date: {date_str}. Error: {e}")
                    return None

            def map_shipment_status(status):
                """Map shipment status to valid enum values"""
                if pd.isna(status) or status == "Not Available":
                    return "Pending"
                status_map = {
                    "Shipped": "Shipped",
                    "Delivered": "Delivered",
                    "Pending": "Pending",
                }
                return status_map.get(status, "Pending")

            def clean_quantity(qty):
                """Ensure quantity is at least 1"""
                try:
                    qty = int(qty)
                    return max(1, qty)
                except (ValueError, TypeError):
                    return 1

            for _, row in items_data.iterrows():
                try:
                    self.db.cursor.execute(
                        insert_query,
                        (
                            row["Order ID"],
                            clean_quantity(row["Quantity"]),
                            float(str(row["Unit Price"]).replace("$", "").replace(",", "")),
                            float(str(row["Unit Price Tax"]).replace("$", "").replace(",", "")),
                            map_shipment_status(row["Shipment Status"]),
                            parse_date(row["Ship Date"]),
                            row["ASIN"],
                        ),
                    )
                except Exception as e:
                    logging.error(f"Error processing row: {row}. Error: {e}")
                    continue

            self.db.conn.commit()
            logging.info(f"Inserted {len(items_data)} order items")

        except Exception as e:
            logging.error(f"Error processing order items: {e}")
            self.db.conn.rollback()
            raise

    def process_addresses(self, df):
        """Process and insert shipping/billing addresses"""
        try:
            # Check for required columns
            required_columns = ['Shipping Address', 'Billing Address', 'Order ID']
            if not all(col in df.columns for col in required_columns):
                logging.error(f"Missing required columns. Available columns: {df.columns.tolist()}")
                return
            # Process unique addresses
            all_addresses = set()
            # Collect all unique addresses from both shipping and billing
            for _, row in df.iterrows():
                shipping_addr = row['Shipping Address']
                billing_addr = row['Billing Address']
                if pd.notna(shipping_addr):
                    all_addresses.add(('shipping', shipping_addr))
                if pd.notna(billing_addr):
                    all_addresses.add(('billing', billing_addr))
            logging.info(f"Processing {len(all_addresses)} unique addresses")

            # Insert addresses
            insert_query = """
                INSERT INTO addresses (
                    address_line1, address_line2, city, state, postal_code, country
                )
                VALUES %s
                ON CONFLICT (address_line1, city, state, postal_code)
                DO UPDATE SET
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id, address_line1, city, state, postal_code
            """
            # Parse and prepare addresses for insertion
            address_tuples = []
            for addr_type, addr in all_addresses:
                logging.info(f"Address: {addr}")
                parsed = self.parse_address((addr, addr_type))
                logging.info(f"Parsed address: {parsed}")
                if parsed:
                    logging.info(f"Parsed address: {parsed}")
                    address_tuples.append(parsed)

            if not address_tuples:
                logging.warning("No valid addresses to insert")
                return

            # Insert addresses and get their IDs
            execute_values(self.db.cursor, insert_query, address_tuples, fetch=True)
            self.db.conn.commit()

            # Create a lookup dictionary for addresses
            address_lookup = {}
            for row in self.db.cursor.fetchall():
                addr_id, addr_line1, city, state, postal_code = row
                key = f"{addr_line1}|{city}|{state}|{postal_code}"
                address_lookup[key] = addr_id

            # Now link addresses to orders
            order_address_insert = """
                INSERT INTO order_addresses (order_id, shipping_address_id, billing_address_id)
                VALUES %s
                ON CONFLICT (order_id)
                DO UPDATE SET
                    shipping_address_id = EXCLUDED.shipping_address_id,
                    billing_address_id = EXCLUDED.billing_address_id
            """
            order_address_data = []
            for _, row in df.iterrows():
                shipping_parsed = self.parse_address((row['Shipping Address'], 'shipping'))
                billing_parsed = self.parse_address((row['Billing Address'], 'billing'))
                if shipping_parsed and billing_parsed:
                    shipping_key = f"{shipping_parsed[0]}|{shipping_parsed[2]}|{shipping_parsed[3]}|{shipping_parsed[4]}"
                    billing_key = f"{billing_parsed[0]}|{billing_parsed[2]}|{billing_parsed[3]}|{billing_parsed[4]}"
                    shipping_id = address_lookup.get(shipping_key)
                    billing_id = address_lookup.get(billing_key)
                    if shipping_id and billing_id:
                        order_address_data.append((
                            row['Order ID'],
                            shipping_id,
                            billing_id
                        ))

            if order_address_data:
                execute_values(self.db.cursor, order_address_insert, order_address_data)
                self.db.conn.commit()
                logging.info(f"Linked {len(order_address_data)} orders with addresses")

        except Exception as e:
            logging.error(f"Error processing addresses: {e}")
            self.db.conn.rollback()
            raise

    def parse_address(self, address_info):
        """Parse address string into components"""
        addr_str, addr_type = address_info
        logging.info(f"Address info: {address_info}")
        if pd.isna(addr_str) or addr_str == "Not Available":
            return None

        try:
            # Remove any 'Shipping Address:' or 'Billing Address:' prefixes
            addr_str = re.sub(
                r"^(Shipping|Billing)\s+Address:\s*", "", addr_str, flags=re.IGNORECASE
            )
            # Split by spaces
            parts = addr_str.split()
            # Extract country (assuming it's always at the end and is "United States")
            country = "United States"
            if parts[-2:] == ["United", "States"]:
                parts = parts[:-2]
            # Extract ZIP code (assuming it's the last part before country)
            postal_code = parts[-1]
            if '-' in postal_code:  # Handle ZIP+4 format
                postal_code = postal_code.split('-')[0]
            parts = parts[:-1]
            # Extract state (assuming it's 2 letters before ZIP)
            state = parts[-1]
            parts = parts[:-1]
            # Extract city (assuming it's one word before state)
            city = parts[-1]
            parts = parts[:-1]
            # Everything before the city is the street address
            # Look for common street identifiers to split address_line2
            street_identifiers = ['DR', 'ST', 'AVE', 'BLVD', 'RD', 'LN', 'CT', 'WAY']
            # Join remaining parts back to a string
            remaining = ' '.join(parts)
            # Find the last occurrence of a street identifier
            address_parts = remaining.split()
            split_index = None
            for i, word in enumerate(address_parts):
                if word in street_identifiers:
                    split_index = i + 1
            if split_index:
                address_line1 = ' '.join(address_parts[:split_index])
                address_line2 = ' '.join(address_parts[split_index:]) if split_index < len(address_parts) else None
            else:
                address_line1 = remaining
                address_line2 = None

            return (
                address_line1,
                address_line2,
                city,
                state,
                postal_code,
                country
            )
        except Exception as e:
            logging.warning(f"Error parsing address: {addr_str}. Error: {e}")
            return None

        return None


def main():
    importer = OrdersImporter(db)
    importer.import_orders_from_csv()


if __name__ == "__main__":
    main()
