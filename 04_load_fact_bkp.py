# ============================================================
# 04_load_fact.py
# Project : Retail Sales Analytics Pipeline
# Purpose : Load FACT_SALES table by joining staging data
#           with dimension surrogate keys
#
# Informatica Mapping Equivalent:
#   Multiple Lookup transformations (one per dimension)
#   → Expression transformation (derive measures)
#   → Aggregator (payment join)
#   → Target (fact_sales)
#
# Load Strategy : Full load for initial run
#                 Incremental (watermark-based) for daily runs
#
# Author  : Manohar Marepally
# ============================================================

import pandas as pd
import numpy as np
import sys
import os
from datetime import datetime, date

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')
))
from etl.utils.db_connector import get_connection, get_engine

STAGING_PATH = 'data/staging/'
LOG_PATH     = 'logs/'
TODAY        = date.today()
RUN_START    = datetime.now()
BATCH_SIZE   = 5000    # insert records in batches to avoid memory issues

print("=" * 60)
print("PHASE 4: LOADING FACT_SALES TABLE")
print(f"Run started at: {RUN_START.strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

conn   = get_connection()
engine = get_engine()
cursor = conn.cursor()


# ============================================================
# HELPER: LOG ETL RUN
# ============================================================
def log_etl_run(table, records_loaded, records_rejected,
                status, error_msg=None):
    cursor.execute("""
        INSERT INTO etl_control_log
        (target_table, run_start_ts, run_end_ts,
         records_loaded, records_rejected,
         last_loaded_ts, status, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        table, RUN_START, datetime.now(),
        records_loaded, records_rejected,
        datetime.now(), status, error_msg
    ))
    conn.commit()


# ============================================================
# HELPER: GET LAST WATERMARK
# Purpose : For incremental loads — get last successful run
#           Mirrors Informatica's high-watermark strategy
# ============================================================
def get_last_watermark():
    cursor.execute("""
        SELECT MAX(last_loaded_ts)
        FROM etl_control_log
        WHERE target_table = 'fact_sales'
        AND status = 'SUCCESS'
    """)
    result = cursor.fetchone()[0]
    return result


# ============================================================
# STEP 1: CHECK LOAD MODE
# Full load vs Incremental load decision
# ============================================================
print("\n" + "=" * 60)
print("STEP 1: DETERMINING LOAD MODE")
print("=" * 60)

cursor.execute("SELECT COUNT(*) FROM fact_sales")
existing_fact_rows = cursor.fetchone()[0]

last_watermark = get_last_watermark()

if existing_fact_rows == 0:
    LOAD_MODE = 'FULL'
    print(f"  Fact table is empty → FULL LOAD mode")
else:
    LOAD_MODE = 'INCREMENTAL'
    print(f"  Existing fact rows  : {existing_fact_rows:,}")
    print(f"  Last watermark      : {last_watermark}")
    print(f"  → INCREMENTAL LOAD mode")


# ============================================================
# STEP 2: LOAD STAGING FILES
# ============================================================
print("\n" + "=" * 60)
print("STEP 2: LOADING STAGING FILES")
print("=" * 60)

df_items    = pd.read_csv(STAGING_PATH + 'stg_items.csv')
df_orders   = pd.read_csv(STAGING_PATH + 'stg_orders.csv')
df_payments = pd.read_csv(STAGING_PATH + 'stg_payments.csv')

print(f"  stg_items    loaded : {len(df_items):>7,} rows")
print(f"  stg_orders   loaded : {len(df_orders):>7,} rows")
print(f"  stg_payments loaded : {len(df_payments):>7,} rows")

# Convert purchase timestamp for watermark filtering
df_orders['order_purchase_timestamp'] = pd.to_datetime(
    df_orders['order_purchase_timestamp']
)

# Apply watermark filter for incremental loads
if LOAD_MODE == 'INCREMENTAL' and last_watermark:
    df_orders_filtered = df_orders[
        df_orders['order_purchase_timestamp'] > pd.Timestamp(last_watermark)
    ]
    print(f"\n  After watermark filter: {len(df_orders_filtered):,} new orders")
else:
    df_orders_filtered = df_orders
    print(f"\n  Full load: processing all {len(df_orders_filtered):,} orders")


# ============================================================
# STEP 3: LOAD DIMENSION LOOKUP TABLES INTO MEMORY
# Purpose : Resolve business keys → surrogate keys
#
# Informatica Equivalent:
#   One Lookup transformation per dimension
#   Input port  = business key (product_id, seller_id etc.)
#   Output port = surrogate key (product_key, seller_key etc.)
#
# We load dims into Python dictionaries for fast O(1) lookup
# Much faster than querying MySQL row-by-row
# ============================================================
print("\n" + "=" * 60)
print("STEP 3: LOADING DIMENSION LOOKUP MAPS")
print("=" * 60)

# Customer lookup: customer_id → customer_key (surrogate)
cursor.execute("""
    SELECT customer_id, customer_key
    FROM dim_customer
    WHERE is_current = TRUE
""")
customer_map = {row[0]: row[1] for row in cursor.fetchall()}
print(f"  Customer lookup map : {len(customer_map):>7,} entries")

# Product lookup: product_id → product_key
cursor.execute("""
    SELECT product_id, product_key
    FROM dim_product
""")
product_map = {row[0]: row[1] for row in cursor.fetchall()}
print(f"  Product lookup map  : {len(product_map):>7,} entries")

# Seller lookup: seller_id → seller_key
cursor.execute("""
    SELECT seller_id, seller_key
    FROM dim_seller
""")
seller_map = {row[0]: row[1] for row in cursor.fetchall()}
print(f"  Seller lookup map   : {len(seller_map):>7,} entries")


# ============================================================
# STEP 4: BUILD FACT DATASET
# Purpose : Join all staging tables and resolve surrogate keys
#
# Join sequence:
#   stg_items
#   → JOIN stg_orders   (on order_id) to get customer_id, date_key
#   → JOIN stg_payments (on order_id) to get payment info
#   → MAP customer_id   → customer_key  (lookup)
#   → MAP product_id    → product_key   (lookup)
#   → MAP seller_id     → seller_key    (lookup)
# ============================================================
print("\n" + "=" * 60)
print("STEP 4: BUILDING FACT DATASET")
print("=" * 60)

# Filter items to only orders in our filtered set
filtered_order_ids = set(df_orders_filtered['order_id'])
df_items_filtered  = df_items[df_items['order_id'].isin(filtered_order_ids)]
print(f"  Items after order filter    : {len(df_items_filtered):,}")

# Join items with orders to get customer_id and order metadata
df_fact = df_items_filtered.merge(
    df_orders_filtered[[
        'order_id', 'customer_id', 'date_key',
        'order_year', 'order_month', 'order_quarter',
        'order_status', 'status_category',
        'delivery_days', 'delivery_delay_days', 'is_on_time',
        'order_purchase_timestamp'
    ]],
    on='order_id',
    how='left'
)
print(f"  After joining orders        : {len(df_fact):,}")

# Join with payments
df_fact = df_fact.merge(
    df_payments[[
        'order_id', 'primary_payment_type',
        'payment_installments', 'total_payment_value'
    ]],
    on='order_id',
    how='left'
)
print(f"  After joining payments      : {len(df_fact):,}")

# ── Resolve surrogate keys using lookup maps ──────────────────
# Mirrors Informatica Lookup transformation output port

df_fact['customer_key'] = df_fact['customer_id'].map(customer_map)
df_fact['product_key']  = df_fact['product_id'].map(product_map)
df_fact['seller_key']   = df_fact['seller_id'].map(seller_map)

# ── Check for lookup failures (no-match condition) ────────────
# In Informatica: these go to the 'no match' output port
lookup_failures = df_fact[
    df_fact['customer_key'].isnull() |
    df_fact['product_key'].isnull()  |
    df_fact['seller_key'].isnull()
]

print(f"\n  Surrogate key lookup failures : {len(lookup_failures):,}")

if len(lookup_failures) > 0:
    lookup_failures.to_csv(
        LOG_PATH + 'fact_lookup_failures.csv', index=False
    )
    print(f"  Failures saved → logs/fact_lookup_failures.csv")

# Remove lookup failures from load set
df_fact_clean = df_fact[
    df_fact['customer_key'].notnull() &
    df_fact['product_key'].notnull()  &
    df_fact['seller_key'].notnull()
].copy()

print(f"  Clean fact records to load  : {len(df_fact_clean):,}")

# ── Convert surrogate keys to integer ────────────────────────
df_fact_clean['customer_key'] = df_fact_clean['customer_key'].astype(int)
df_fact_clean['product_key']  = df_fact_clean['product_key'].astype(int)
df_fact_clean['seller_key']   = df_fact_clean['seller_key'].astype(int)
df_fact_clean['date_key']     = df_fact_clean['date_key'].astype(int)

# Add ETL load date
df_fact_clean['etl_load_date'] = TODAY


# ============================================================
# STEP 5: SELECT FINAL COLUMNS FOR FACT TABLE
# Only include columns that exist in fact_sales schema
# ============================================================
fact_columns = [
    'date_key',
    'customer_key',
    'product_key',
    'seller_key',
    'order_id',
    'order_item_id',        # line item sequence
    'unit_price',
    'freight_value',
    'gross_revenue',
    'total_cost',
    'delivery_days',
    'delivery_delay_days',
    'is_on_time',
    'primary_payment_type',
    'payment_installments',
    'total_payment_value',
    'order_status',
    'status_category',
    'etl_load_date'
]

# Rename order_item_id to match schema column name
df_fact_clean = df_fact_clean.rename(
    columns={'order_item_id': 'order_item_seq',
             'price': 'unit_price'}
)

# Rebuild fact_columns list with renamed column
fact_columns = [
    'date_key', 'customer_key', 'product_key', 'seller_key',
    'order_id', 'order_item_seq',
    'unit_price', 'freight_value', 'gross_revenue', 'total_cost',
    'delivery_days', 'delivery_delay_days', 'is_on_time',
    'primary_payment_type', 'payment_installments', 'total_payment_value',
    'order_status', 'status_category', 'etl_load_date'
]

# Keep only columns that exist in our dataframe
available_cols = [c for c in fact_columns if c in df_fact_clean.columns]
df_final = df_fact_clean[available_cols].copy()

print(f"\n  Final fact columns selected : {len(available_cols)}")
print(f"  Final fact rows to load     : {len(df_final):,}")


# ============================================================
# STEP 6: LOAD FACT TABLE IN BATCHES
# Purpose : Batch inserts prevent memory overload
#           Essential for large fact tables
#           In Informatica: controlled by commit interval
# ============================================================
print("\n" + "=" * 60)
print("STEP 6: LOADING FACT_SALES IN BATCHES")
print("=" * 60)

try:
    # For full load: truncate first
    if LOAD_MODE == 'FULL':
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        cursor.execute("TRUNCATE TABLE fact_sales")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        conn.commit()
        print(f"  Fact table truncated for full load")

    total_loaded  = 0
    total_batches = (len(df_final) // BATCH_SIZE) + 1

    for batch_num in range(total_batches):
        start_idx = batch_num * BATCH_SIZE
        end_idx   = start_idx + BATCH_SIZE
        batch_df  = df_final.iloc[start_idx:end_idx]

        if len(batch_df) == 0:
            break

        # Replace NaN with None for MySQL compatibility
        batch_df = batch_df.where(pd.notnull(batch_df), None)

        batch_df.to_sql(
            'fact_sales',
            con=engine,
            if_exists='append',
            index=False,
            method='multi'      # faster bulk insert
        )

        total_loaded += len(batch_df)

        # Progress indicator every 5 batches
        if (batch_num + 1) % 5 == 0 or batch_num == total_batches - 1:
            print(f"  Batch {batch_num + 1:>3}/{total_batches} "
                  f"→ {total_loaded:>7,} records loaded")

    conn.commit()
    print(f"\n  Total records loaded into fact_sales: {total_loaded:,}")
    log_etl_run('fact_sales', total_loaded, len(lookup_failures), 'SUCCESS')

except Exception as e:
    print(f"  ERROR loading fact_sales: {e}")
    log_etl_run('fact_sales', 0, 0, 'FAILED', str(e))
    conn.rollback()
    raise


# ============================================================
# STEP 7: VERIFICATION QUERIES
# Purpose : Confirm data loaded correctly
#           Always verify — never assume
# ============================================================
print("\n" + "=" * 60)
print("STEP 7: FACT TABLE VERIFICATION")
print("=" * 60)

# Total row count
cursor.execute("SELECT COUNT(*) FROM fact_sales")
fact_count = cursor.fetchone()[0]
print(f"\n  Total rows in fact_sales    : {fact_count:,}")

# Total revenue
cursor.execute("SELECT ROUND(SUM(gross_revenue), 2) FROM fact_sales")
total_revenue = cursor.fetchone()[0]
print(f"  Total gross revenue         : R$ {total_revenue:,.2f}")

# Revenue by year
print(f"\n  Revenue by year:")
cursor.execute("""
    SELECT d.year,
           COUNT(DISTINCT f.order_id)      AS orders,
           ROUND(SUM(f.gross_revenue), 2)  AS revenue
    FROM fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.year
    ORDER BY d.year
""")
for row in cursor.fetchall():
    print(f"    {row[0]}  →  {row[1]:>6,} orders  |  "
          f"R$ {row[2]:>12,.2f} revenue")

# Revenue by order status
print(f"\n  Revenue by status category:")
cursor.execute("""
    SELECT status_category,
           COUNT(*)                        AS records,
           ROUND(SUM(gross_revenue), 2)    AS revenue
    FROM fact_sales
    GROUP BY status_category
    ORDER BY revenue DESC
""")
for row in cursor.fetchall():
    print(f"    {str(row[0]):15}  →  {row[1]:>7,} records  |  "
          f"R$ {row[2]:>12,.2f}")

# Top 5 product categories by revenue
print(f"\n  Top 5 product categories:")
cursor.execute("""
    SELECT p.product_category,
           ROUND(SUM(f.gross_revenue), 2)  AS revenue,
           COUNT(DISTINCT f.order_id)      AS orders
    FROM fact_sales f
    JOIN dim_product p ON f.product_key = p.product_key
    GROUP BY p.product_category
    ORDER BY revenue DESC
    LIMIT 5
""")
for row in cursor.fetchall():
    print(f"    {str(row[0]):30}  →  R$ {row[1]:>10,.2f}  "
          f"({row[2]:,} orders)")

# Payment type split
print(f"\n  Revenue by payment type:")
cursor.execute("""
    SELECT primary_payment_type,
           COUNT(*)                        AS transactions,
           ROUND(SUM(gross_revenue), 2)    AS revenue
    FROM fact_sales
    GROUP BY primary_payment_type
    ORDER BY revenue DESC
""")
for row in cursor.fetchall():
    print(f"    {str(row[0]):15}  →  {row[1]:>7,} txns  |  "
          f"R$ {row[2]:>12,.2f}")

# ETL control log summary
print(f"\n  ETL Control Log (all runs):")
cursor.execute("""
    SELECT target_table, status,
           records_loaded, run_end_ts
    FROM etl_control_log
    ORDER BY log_id DESC
    LIMIT 8
""")
for row in cursor.fetchall():
    print(f"    {str(row[0]):20} | {str(row[1]):8} | "
          f"{str(row[2]):>7} records | {str(row[3])}")


# ============================================================
# CLOSE CONNECTION
# ============================================================
cursor.close()
conn.close()

print("\n" + "=" * 60)
print("  PHASE 4 COMPLETE")
print("  Full pipeline execution successful")
print("  Star schema is ready for SQL analytics")
print("  Next: Phase 5 - Advanced SQL Queries")
print("=" * 60)