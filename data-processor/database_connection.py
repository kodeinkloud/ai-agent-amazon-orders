import psycopg2
from contextlib import contextmanager

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

# Default database configuration
db_config = {
    "dbname": "database-name",
    "user": "username",
    "password": "password",
    "host": "localhost",
    "port": "5432"
}

# Create a default database connection instance
db = DatabaseConnection(db_config)
