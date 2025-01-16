import psycopg2
from config import DB_CONFIG

class DatabaseConnection:
    def __init__(self):
        self.conn = None
        self.cursor = None

    def connect_to_db(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.cursor = self.conn.cursor()
            # Reset any failed transaction
            self.conn.rollback()
            # logging.info("Successfully connected to database")
        except Exception as e:
            # logging.error(f"Error connecting to database: {e}")
            raise

    def close_connection(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            # logging.info("Database connection closed")

    def commit(self):
        """Commit the current transaction"""
        self.conn.commit()

    def rollback(self):
        """Rollback the current transaction"""
        self.conn.rollback()

# Create a single instance to be used throughout the application
db = DatabaseConnection()