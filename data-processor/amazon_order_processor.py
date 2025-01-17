import logging
import pandas as pd
from config import ORDERS_CSV_FILE_PATH
from database_connection import db
from products import Products
from orders import Orders
from order_items import OrderItems
from addresses import Addresses

# Add this after the imports
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class OrdersImporter:
    def __init__(self, db_connection):
        self.db = db_connection
        self.db.connect_to_db()
        self.products = Products(db_connection)
        self.orders = Orders(db_connection)
        self.order_items = OrderItems(db_connection)
        self.addresses = Addresses(db_connection)

    def import_orders_from_csv(self):
        df = pd.read_csv(ORDERS_CSV_FILE_PATH)
        self.products.process_products(df)
        self.orders.process_orders(df)
        self.order_items.process_order_items(df)
        self.addresses.process_addresses(df)

def main():
    importer = OrdersImporter(db)
    importer.import_orders_from_csv()


if __name__ == "__main__":
    main()
