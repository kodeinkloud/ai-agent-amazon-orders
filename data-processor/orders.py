import logging

import pandas as pd
from config import ORDERS_CSV_FILE_PATH
from database_connection import db
from psycopg2.extras import execute_values


class OrdersImporter:
    def __init__(self, db_connection):
        self.db = db_connection
        self.db.connect_to_db()

    def import_orders_from_csv(self):
        logging.info(ORDERS_CSV_FILE_PATH)
        df = pd.read_csv(ORDERS_CSV_FILE_PATH)
        self.process_products(df)
        self.process_orders(df)
        self.process_order_items(df)

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


def main():
    importer = OrdersImporter(db)
    importer.import_orders_from_csv()


if __name__ == "__main__":
    main()
