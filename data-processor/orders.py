import logging
import pandas as pd
from addresses import Addresses

# Add this after the imports
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class Orders:
    def __init__(self, db_connection):
        self.db = db_connection
        self.addresses = Addresses(db_connection)

    def process_orders(self, df):
        """Process and insert orders data"""
        try:
            orders_data = df[
                [
                    "Order ID", "Website", "Order Date", "Currency",
                    "Total Owed", "Shipping Charge", "Total Discounts",
                    "Shipping Address", "Billing Address"
                ]
            ].drop_duplicates()

            def clean_monetary_value(value):
                try:
                    cleaned = str(value).replace("$", "").replace(",", "").replace('"', "").replace("'", "")
                    return float(cleaned)
                except (ValueError, AttributeError):
                    logging.warning(f"Could not convert value: {value}, defaulting to 0.0")
                    return 0.0

            for _, row in orders_data.iterrows():
                try:
                    # Get address IDs
                    shipping_parsed = self.addresses.parse_address((row['Shipping Address'], 'shipping'))
                    billing_parsed = self.addresses.parse_address((row['Billing Address'], 'billing'))
                    
                    shipping_address_id = None
                    billing_address_id = None

                    if shipping_parsed:
                        # Get shipping address ID
                        self.db.cursor.execute("""
                            SELECT id FROM addresses
                            WHERE address_line1 = %s
                            AND city = %s
                            AND state = %s
                            AND postal_code = %s
                        """, (shipping_parsed[0], shipping_parsed[2], shipping_parsed[3], shipping_parsed[4]))
                        result = self.db.cursor.fetchone()
                        shipping_address_id = result[0] if result else None

                    if billing_parsed:
                        # Get billing address ID
                        self.db.cursor.execute("""
                            SELECT id FROM addresses
                            WHERE address_line1 = %s
                            AND city = %s
                            AND state = %s
                            AND postal_code = %s
                        """, (billing_parsed[0], billing_parsed[2], billing_parsed[3], billing_parsed[4]))
                        result = self.db.cursor.fetchone()
                        billing_address_id = result[0] if result else None

                    # Insert order with address IDs
                    self.db.cursor.execute("""
                        INSERT INTO orders (
                            order_id, website, order_date, currency,
                            shipping_address_id, billing_address_id,
                            total_owed, shipping_charge, total_discounts
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (order_id)
                        DO UPDATE SET
                            shipping_address_id = EXCLUDED.shipping_address_id,
                            billing_address_id = EXCLUDED.billing_address_id,
                            total_owed = EXCLUDED.total_owed,
                            shipping_charge = EXCLUDED.shipping_charge,
                            total_discounts = EXCLUDED.total_discounts,
                            updated_at = CURRENT_TIMESTAMP
                    """, (
                        row["Order ID"],
                        row["Website"],
                        pd.to_datetime(row["Order Date"]),
                        row["Currency"],
                        shipping_address_id,
                        billing_address_id,
                        clean_monetary_value(row["Total Owed"]),
                        clean_monetary_value(row["Shipping Charge"]),
                        clean_monetary_value(row["Total Discounts"])
                    ))

                except Exception as e:
                    logging.error(f"Error processing order {row['Order ID']}: {e}")
                    continue

            self.db.conn.commit()
            logging.info(f"Processed {len(orders_data)} orders")

        except Exception as e:
            logging.error(f"Error processing orders: {e}")
            self.db.conn.rollback()
            raise
