import os

# Base directory of the project
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Data directory paths
DATA_DIR = os.path.join(BASE_DIR, 'data')
ORDERS_CSV_FILE_PATH = os.path.join(DATA_DIR, 'orders.csv')

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'database-name',
    'user': 'username',
    'password': 'password'
} 