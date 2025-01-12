import psycopg2
from contextlib import contextmanager
from config import DB_CONFIG

class DatabaseConnection:
    def __init__(self, db_params):
        self.db_params = db_params

    @contextmanager
    def get_cursor(self):
        conn = None
        try:
            # Connect to database
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            yield cursor
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

# Create a default database connection instance
db = DatabaseConnection(DB_CONFIG)