# ============================================================
# 04_load_fact.py
# Project : Retail Sales Analytics Pipeline
# Purpose : Load FACT_SALES table by joining staging data
#           with dimension surrogate keys
# Author  : Manohar
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
BATCH_SIZE   = 5000

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
# STEP 1: DETERMINE LOAD MODE
# ============================================================
print("\n" + "=" * 60)
print("STEP 1: DETERMINING LOAD MODE")
print("=" * 60)

cursor.execute("SELECT COUNT(*) FROM fact_sales")
existing_fact_rows = cursor.fetchone()[0]
last_watermark     = get_last_watermark()

if existing_fact_rows == 0:
    LOAD_MODE = 'FULL'
    print(f"  Fact table is empty --> FULL LOAD mode")
else:
    LOAD_MODE = 'INCREMENTAL'
    print(f"  Existing fact rows  : {existing_fact_rows:,}")
    print(f"  Last watermark      : {last_watermark}")
    print(f"  --> INCREMENTAL LOAD mode")


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
    print(f"\n  After watermark filter : {len(df_orders_filtered):,} new orders")
else:
    df_orders_filtered = df_orders
    print(f"\n  Full load: processing all {len(df_orders_filtered):,} orders")


# ============================================================
# STEP 3: LOAD DIMENSION LOOKUP MAPS INTO MEMORY
# Mirrors Informatica Lookup transformation
# business key in --> surrogate key out
# ============================================================
print("\n" + "=" * 60)
print("STEP 3: LOADING DIMENSION LOOKUP MAPS")
print("=" * 60)

cursor.execute("""
    SELECT customer_id, customer_key
    FROM dim_customer
    WHERE is_current = TRUE
""")
customer_map = {row[0]: row[1] for row in cursor.fetchall()}
print(f"  Customer lookup map : {len(customer_map):>7,} entries")

cursor.execute("SELECT product_id, product_key FROM dim_product")
product_map = {row[0]: row[1] for row in cursor.fetchall()}
print(f"  Product lookup map  : {len(product_map):>7,} entries")

cursor.execute("SELECT seller_id, seller_key FROM dim_seller")
seller_map = {row[0]: row[1] for row in cursor.fetchall()}
print(f"  Seller lookup map   : {len(seller_map):>7,} entries")


# ============================================================
# STEP 4: BUILD FACT DATASET
# ============================================================
print("\n" + "=" * 60)
print("STEP 4: BUILDING FACT DATASET")
print("=" * 60)

# Filter items to only orders in our filtered set
filtered_order_ids = set(df_orders_filtered['order_id'])
df_items_filtered  = df_items[
    df_items['order_id'].isin(filtered_order_ids)
].copy()
print(f"  Items after order filter    : {len(df_items_filtered):,}")

# ── Drop conflicting columns from items ───────────────────────
# Both stg_items and stg_orders have these columns from Phase 2
# Drop from items, re-join cleanly from orders
# Informatica equivalent: removing duplicate input ports
# before a Joiner transformation

conflict_cols = [
    'date_key', 'order_year', 'order_month', 'order_quarter',
    'is_on_time', 'delivery_days', 'status_category'
]
cols_to_drop = [c for c in conflict_cols
                if c in df_items_filtered.columns]
df_items_filtered = df_items_filtered.drop(columns=cols_to_drop)
print(f"  Dropped {len(cols_to_drop)} conflicting columns from items")

# ── Join items → orders ───────────────────────────────────────
order_cols = [
    'order_id', 'customer_id', 'date_key',
    'order_year', 'order_month', 'order_quarter',
    'order_status', 'status_category',
    'delivery_days', 'delivery_delay_days', 'is_on_time',
    'order_purchase_timestamp'
]

df_fact = df_items_filtered.merge(
    df_orders_filtered[order_cols],
    on='order_id',
    how='left'
)
print(f"  After joining orders        : {len(df_fact):,}")

# Confirm no _x _y conflicts
conflict_check = [c for c in df_fact.columns
                  if c.endswith('_x') or c.endswith('_y')]
if conflict_check:
    print(f"  WARNING conflicts: {conflict_check}")
else:
    print(f"  No column conflicts - clean merge")

# ── Join → payments ───────────────────────────────────────────
df_fact = df_fact.merge(
    df_payments[[
        'order_id', 'primary_payment_type',
        'payment_installments', 'total_payment_value'
    ]],
    on='order_id',
    how='left'
)
print(f"  After joining payments      : {len(df_fact):,}")

# ── Key column validation ─────────────────────────────────────
print(f"\n  Key column validation:")
required_cols = [
    'date_key', 'customer_id', 'product_id',
    'seller_id', 'gross_revenue', 'delivery_delay_days'
]
for col in required_cols:
    if col in df_fact.columns:
        nulls = df_fact[col].isnull().sum()
        print(f"    {col:25} --> exists | nulls: {nulls:,}")
    else:
        print(f"    {col:25} --> MISSING")

# ── Resolve surrogate keys ────────────────────────────────────
df_fact['customer_key'] = df_fact['customer_id'].map(customer_map)
df_fact['product_key']  = df_fact['product_id'].map(product_map)
df_fact['seller_key']   = df_fact['seller_id'].map(seller_map)

# ── Lookup failure check ──────────────────────────────────────
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
    print(f"  Failures saved --> logs/fact_lookup_failures.csv")

# ── Remove failures ───────────────────────────────────────────
df_fact_clean = df_fact[
    df_fact['customer_key'].notnull() &
    df_fact['product_key'].notnull()  &
    df_fact['seller_key'].notnull()
].copy()

print(f"  Clean fact records to load  : {len(df_fact_clean):,}")

# ── Convert keys to integer ───────────────────────────────────
df_fact_clean['customer_key'] = df_fact_clean['customer_key'].astype(int)
df_fact_clean['product_key']  = df_fact_clean['product_key'].astype(int)
df_fact_clean['seller_key']   = df_fact_clean['seller_key'].astype(int)
df_fact_clean['date_key']     = df_fact_clean['date_key'].astype(int)
df_fact_clean['etl_load_date'] = TODAY


# ============================================================
# STEP 5: RENAME AND SELECT FINAL COLUMNS
# ============================================================
print("\n" + "=" * 60)
print("STEP 5: PREPARING FINAL COLUMNS")
print("=" * 60)

rename_map = {
    'order_item_id' : 'order_item_seq',
    'price'         : 'unit_price'
}
rename_actual = {k: v for k, v in rename_map.items()
                 if k in df_fact_clean.columns}
df_fact_clean = df_fact_clean.rename(columns=rename_actual)
print(f"  Renamed columns : {rename_actual}")

fact_columns = [
    'date_key',
    'customer_key',
    'product_key',
    'seller_key',
    'order_id',
    'order_item_seq',
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

available_cols = [c for c in fact_columns if c in df_fact_clean.columns]
missing_cols   = [c for c in fact_columns if c not in df_fact_clean.columns]

if missing_cols:
    print(f"  Missing columns (skipping) : {missing_cols}")

df_final = df_fact_clean[available_cols].copy()
print(f"  Final columns : {len(available_cols)}")
print(f"  Final rows    : {len(df_final):,}")


# ============================================================
# STEP 6: LOAD FACT TABLE IN BATCHES
# ============================================================
print("\n" + "=" * 60)
print("STEP 6: LOADING FACT_SALES IN BATCHES")
print("=" * 60)

try:
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
        batch_df  = df_final.iloc[start_idx:end_idx].copy()

        if len(batch_df) == 0:
            break

        batch_df = batch_df.where(pd.notnull(batch_df), None)

        batch_df.to_sql(
            'fact_sales',
            con=engine,
            if_exists='append',
            index=False,
            method='multi'
        )

        total_loaded += len(batch_df)

        if (batch_num + 1) % 5 == 0 or batch_num == total_batches - 1:
            print(f"  Batch {batch_num + 1:>3}/{total_batches} "
                  f"--> {total_loaded:>7,} records loaded")

    conn.commit()
    print(f"\n  Total records loaded : {total_loaded:,}")
    log_etl_run('fact_sales', total_loaded, len(lookup_failures), 'SUCCESS')

except Exception as e:
    print(f"  ERROR: {e}")
    log_etl_run('fact_sales', 0, 0, 'FAILED', str(e))
    conn.rollback()
    raise


# ============================================================
# STEP 7: VERIFICATION QUERIES
# ============================================================
print("\n" + "=" * 60)
print("STEP 7: FACT TABLE VERIFICATION")
print("=" * 60)

cursor.execute("SELECT COUNT(*) FROM fact_sales")
print(f"\n  Total rows in fact_sales    : {cursor.fetchone()[0]:,}")

cursor.execute("SELECT ROUND(SUM(gross_revenue), 2) FROM fact_sales")
print(f"  Total gross revenue         : R$ {cursor.fetchone()[0]:,.2f}")

print(f"\n  Revenue by year:")
cursor.execute("""
    SELECT d.year,
           COUNT(DISTINCT f.order_id)     AS orders,
           ROUND(SUM(f.gross_revenue), 2) AS revenue
    FROM fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.year
    ORDER BY d.year
""")
for row in cursor.fetchall():
    print(f"    {row[0]}  -->  {row[1]:>6,} orders  |  "
          f"R$ {row[2]:>12,.2f}")

print(f"\n  Revenue by status category:")
cursor.execute("""
    SELECT status_category,
           COUNT(*)                       AS records,
           ROUND(SUM(gross_revenue), 2)   AS revenue
    FROM fact_sales
    GROUP BY status_category
    ORDER BY revenue DESC
""")
for row in cursor.fetchall():
    print(f"    {str(row[0]):15} -->  {row[1]:>7,} records  |  "
          f"R$ {row[2]:>12,.2f}")

print(f"\n  Top 5 product categories:")
cursor.execute("""
    SELECT p.product_category,
           ROUND(SUM(f.gross_revenue), 2) AS revenue,
           COUNT(DISTINCT f.order_id)     AS orders
    FROM fact_sales f
    JOIN dim_product p ON f.product_key = p.product_key
    GROUP BY p.product_category
    ORDER BY revenue DESC
    LIMIT 5
""")
for row in cursor.fetchall():
    print(f"    {str(row[0]):35} --> "
          f"R$ {row[1]:>10,.2f}  ({row[2]:,} orders)")

print(f"\n  Revenue by payment type:")
cursor.execute("""
    SELECT primary_payment_type,
           COUNT(*)                       AS transactions,
           ROUND(SUM(gross_revenue), 2)   AS revenue
    FROM fact_sales
    GROUP BY primary_payment_type
    ORDER BY revenue DESC
""")
for row in cursor.fetchall():
    print(f"    {str(row[0]):20} --> "
          f"{row[1]:>7,} txns  |  R$ {row[2]:>12,.2f}")

print(f"\n  ETL Control Log (recent runs):")
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

cursor.close()
conn.close()

print("\n" + "=" * 60)
print("  PHASE 4 COMPLETE")
print("  Full pipeline execution successful")
print("  Star schema is ready for SQL analytics")
print("  Next: Phase 5 - Advanced SQL Queries")
print("=" * 60)