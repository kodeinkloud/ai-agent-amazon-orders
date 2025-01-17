import logging
import re
import pandas as pd
from psycopg2.extras import execute_values

# Add this after the imports
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class Addresses:
    def __init__(self, db_connection):
        self.db = db_connection

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
            # logging.info(f"Processing {len(all_addresses)} unique addresses")

            # Insert addresses
            insert_query = """
                INSERT INTO addresses (
                    address_line1, address_line2, city, state, postal_code, country
                )
                VALUES %s
                ON CONFLICT (address_line1, city, state, postal_code)
                DO UPDATE SET
                    address_line2 = EXCLUDED.address_line2,
                    country = EXCLUDED.country
                RETURNING id, address_line1, city, state, postal_code
            """
            # Parse and prepare addresses for insertion
            address_tuples = []
            seen_addresses = set()  # Track unique addresses
            for addr_type, addr in all_addresses:
                parsed = self.parse_address((addr, addr_type))
                if parsed:
                    # Create a key from the essential address components
                    address_key = (
                        parsed[0].upper(),  # address_line1
                        # parsed[2],  # city
                        # parsed[3],  # state
                        # parsed[4]   # postal_code
                    )
                    # Only add if we haven't seen this address before
                    if address_key not in seen_addresses:
                        seen_addresses.add(address_key)
                        address_tuples.append(parsed)
                        logging.info(f"Added unique address: {parsed}")

            if not address_tuples:
                logging.warning("No valid addresses to insert")
                return
            logging.info(f"Address tuples: {address_tuples}")
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
                # logging.info(f"Linked {len(order_address_data)} orders with addresses")

        except Exception as e:
            logging.error(f"Error processing addresses: {e}")
            self.db.conn.rollback()
            raise

    def parse_address(self, address_info):
        """Parse address string into components"""
        addr_str, addr_type = address_info
        # logging.info(f"Address info: {address_info}")
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