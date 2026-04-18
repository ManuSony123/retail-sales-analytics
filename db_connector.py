# ============================================================
# db_connector.py
# Purpose : Reusable MySQL connection utility
#           Used by all ETL scripts
# ============================================================

import mysql.connector
from sqlalchemy import create_engine

# ── Update these with your MySQL credentials ─────────────────
DB_CONFIG = {
    'host'    : 'localhost',
    'port'    : 3306,
    'database': 'retail_dw',
    'user'    : 'root',
    'password': 'manohar'   # ← change this
}

def get_connection():
    """
    Returns a raw mysql.connector connection.
    Use for INSERT / UPDATE / DELETE operations.
    """
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        print("✓ MySQL connection successful")
        return conn
    except mysql.connector.Error as e:
        print(f"✗ Connection failed: {e}")
        raise

def get_engine():
    """
    Returns a SQLAlchemy engine.
    Use for pandas read_sql() and to_sql() operations.
    """
    url = (
        f"mysql+mysqlconnector://"
        f"{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}"
        f"/{DB_CONFIG['database']}"
    )
    engine = create_engine(url)
    return engine


# ── Quick test when run directly ─────────────────────────────
if __name__ == '__main__':
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT VERSION()")
    version = cursor.fetchone()
    print(f"MySQL version: {version[0]}")
    conn.close()