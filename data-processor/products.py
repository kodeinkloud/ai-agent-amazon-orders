import logging
import pandas as pd
from psycopg2.extras import execute_values

# Add this after the imports
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class Products:
    def __init__(self, db_connection):
        self.db = db_connection

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

            # logging.info(f"Processing {len(products_data)} unique products")

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
            # logging.info(f"Inserted/updated {len(products_data)} products")

        except Exception as e:
            logging.error(f"Error processing products: {e}")
            self.db.conn.rollback()
            raise
