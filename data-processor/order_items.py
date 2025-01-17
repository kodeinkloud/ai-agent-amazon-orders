import logging
import pandas as pd
# from psycopg2.extras import execute_values

# Add this after the imports
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class OrderItems:
    def __init__(self, db_connection):
        self.db = db_connection

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
            # logging.info(f"Inserted {len(items_data)} order items")

        except Exception as e:
            logging.error(f"Error processing order items: {e}")
            self.db.conn.rollback()
            raise