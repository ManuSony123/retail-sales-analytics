# ============================================================
# 03_load_dimensions.py
# Project : Retail Sales Analytics Pipeline
# Purpose : Load all dimension tables from staging files
#           Includes SCD Type 2 logic for dim_customer
#
# Informatica Mapping Equivalent:
#   Lookup → Expression → Router → SCD Target
#
# Author  : Manohar
# ============================================================

import pandas as pd
import numpy as np
import sys
import os
from datetime import datetime, date

# Add project root to path so we can import db_connector
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from etl.utils.db_connector import get_connection, get_engine

STAGING_PATH = 'data/staging/'
LOG_PATH     = 'logs/'
TODAY        = date.today()
RUN_START    = datetime.now()

print("=" * 60)
print("PHASE 3: LOADING DIMENSION TABLES")
print(f"Run started at: {RUN_START.strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

conn   = get_connection()
engine = get_engine()
cursor = conn.cursor()

# ============================================================
# HELPER: SAFE TRUNCATE (bypasses FK constraint temporarily)
# This is standard practice in DWH loading sequences
# ============================================================
def safe_truncate(table_name):
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    conn.commit()
    print(f"  Truncated {table_name}")

# ============================================================
# HELPER: LOG ETL RUN TO CONTROL TABLE
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
# LOAD 1: DIM_DATE
# Purpose : Populate date dimension for full data range
#           Generated programmatically — no source file needed
#           This is standard practice in all data warehouses
# ============================================================
print("\n" + "=" * 60)
print("LOADING DIM_DATE")
print("=" * 60)

try:
    # Check if already loaded
    cursor.execute("SELECT COUNT(*) FROM dim_date")
    existing = cursor.fetchone()[0]

    if existing > 0:
        print(f"  dim_date already has {existing:,} rows. Skipping.")
    else:
        # Generate date range covering full dataset + buffer
        date_range = pd.date_range(start='2016-01-01', end='2019-12-31', freq='D')

        date_records = []
        for d in date_range:
            date_records.append({
                'date_key'      : int(d.strftime('%Y%m%d')),
                'full_date'     : d.date(),
                'year'          : d.year,
                'quarter'       : d.quarter,
                'quarter_name'  : f'Q{d.quarter}',
                'month_num'     : d.month,
                'month_name'    : d.strftime('%B'),
                'month_short'   : d.strftime('%b'),
                'week_of_year'  : int(d.strftime('%W')),
                'day_of_month'  : d.day,
                'day_of_week'   : d.isoweekday(),   # 1=Mon, 7=Sun
                'day_name'      : d.strftime('%A'),
                'is_weekend'    : d.isoweekday() >= 6,
                'is_month_start': d.day == 1,
                'is_month_end'  : d.day == d.days_in_month,
                'year_month'    : d.strftime('%Y-%m')
            })

        df_dates = pd.DataFrame(date_records)
        df_dates.to_sql(
            'dim_date', con=engine,
            if_exists='append', index=False
        )
        print(f"  Loaded {len(df_dates):,} date records into dim_date")
        log_etl_run('dim_date', len(df_dates), 0, 'SUCCESS')

except Exception as e:
    print(f"  ERROR loading dim_date: {e}")
    log_etl_run('dim_date', 0, 0, 'FAILED', str(e))
    raise


# ============================================================
# LOAD 2: DIM_SELLER
# Purpose : Load seller dimension
#           Simple full-load (small table, rarely changes)
# ============================================================
print("\n" + "=" * 60)
print("LOADING DIM_SELLER")
print("=" * 60)

try:
    sellers = pd.read_csv('data/raw/olist_sellers_dataset.csv')

    df_seller = pd.DataFrame({
        'seller_id'        : sellers['seller_id'],
        'seller_city'      : sellers['seller_city'].str.strip().str.title(),
        'seller_state'     : sellers['seller_state'].str.strip().str.upper(),
        'seller_zip_prefix': sellers['seller_zip_code_prefix'].astype(str),
        'etl_load_date'    : TODAY
    })

# Truncate and reload (full load strategy for small dims)
# Disable FK checks to avoid issues if dim_customer references dim_seller
    safe_truncate('dim_seller')

    df_seller.to_sql(
        'dim_seller', con=engine,
        if_exists='append', index=False
    )

    print(f"  Loaded {len(df_seller):,} sellers into dim_seller")
    log_etl_run('dim_seller', len(df_seller), 0, 'SUCCESS')

except Exception as e:
    print(f"  ERROR loading dim_seller: {e}")
    log_etl_run('dim_seller', 0, 0, 'FAILED', str(e))
    raise


# ============================================================
# LOAD 3: DIM_PRODUCT
# Purpose : Load product dimension with derived fields
#           Full load — product catalog rarely changes in bulk
# ============================================================
print("\n" + "=" * 60)
print("LOADING DIM_PRODUCT")
print("=" * 60)

try:
    df_stg_products = pd.read_csv(STAGING_PATH + 'stg_products.csv')

    df_product = pd.DataFrame({
        'product_id'      : df_stg_products['product_id'],
        'product_category': df_stg_products['product_category_name_english'],
        'price_band'      : df_stg_products['price_band'],
        'weight_category' : df_stg_products['weight_category'],
        'product_weight_g': pd.to_numeric(
                                df_stg_products['product_weight_g'],
                                errors='coerce'),
        'product_length_cm': pd.to_numeric(
                                df_stg_products['product_length_cm'],
                                errors='coerce'),
        'product_height_cm': pd.to_numeric(
                                df_stg_products['product_height_cm'],
                                errors='coerce'),
        'product_width_cm' : pd.to_numeric(
                                df_stg_products['product_width_cm'],
                                errors='coerce'),
        'etl_load_date'   : TODAY
    })
# Truncate and reload (full load strategy for small dims)
# Disable FK checks to avoid issues if dim_customer references dim_product
    safe_truncate('dim_product')

    df_product.to_sql(
        'dim_product', con=engine,
        if_exists='append', index=False
    )

    print(f"  Loaded {len(df_product):,} products into dim_product")
    log_etl_run('dim_product', len(df_product), 0, 'SUCCESS')

except Exception as e:
    print(f"  ERROR loading dim_product: {e}")
    log_etl_run('dim_product', 0, 0, 'FAILED', str(e))
    raise


# ============================================================
# LOAD 4: DIM_CUSTOMER with SCD TYPE 2
# Purpose : Load customer dimension tracking history
#
# SCD Type 2 Logic (same as Informatica SCD wizard):
#   CASE 1 - New customer    → INSERT new record
#   CASE 2 - Existing, no change → SKIP (no-op)
#   CASE 3 - Existing, changed  → EXPIRE old + INSERT new
#
# Tracked attributes: customer_city, customer_state
# ============================================================
print("\n" + "=" * 60)
print("LOADING DIM_CUSTOMER (SCD TYPE 2)")
print("=" * 60)

try:
    df_stg_customers = pd.read_csv(STAGING_PATH + 'stg_customers.csv')

    # Check how many customers already exist
    cursor.execute("SELECT COUNT(*) FROM dim_customer")
    existing_count = cursor.fetchone()[0]
    print(f"  Existing records in dim_customer: {existing_count:,}")

    new_count     = 0
    updated_count = 0
    skipped_count = 0

    for _, row in df_stg_customers.iterrows():

        # ── LOOKUP: Does this customer already exist? ─────────
        # Mirrors Informatica Lookup transformation
        cursor.execute("""
            SELECT customer_key, customer_city, customer_state
            FROM dim_customer
            WHERE customer_id = %s AND is_current = TRUE
        """, (row['customer_id'],))

        existing = cursor.fetchone()

        if existing is None:
            # ── CASE 1: New customer → INSERT ─────────────────
            cursor.execute("""
                INSERT INTO dim_customer (
                    customer_id, customer_unique_id,
                    customer_city, customer_state,
                    customer_zip_prefix,
                    effective_start_date, effective_end_date,
                    is_current, record_version, etl_load_date
                ) VALUES (%s, %s, %s, %s, %s, %s, NULL, TRUE, 1, %s)
            """, (
                row['customer_id'],
                row.get('customer_unique_id', row['customer_id']),
                row['customer_city'],
                row['customer_state'],
                str(row.get('customer_zip_code_prefix', '')),
                TODAY, TODAY
            ))
            new_count += 1

        else:
            customer_key, old_city, old_state = existing

            city_changed  = old_city  != row['customer_city']
            state_changed = old_state != row['customer_state']

            if city_changed or state_changed:
                # ── CASE 3: Changed → expire old + insert new ─
                # Step A: Expire the current record
                cursor.execute("""
                    UPDATE dim_customer
                    SET effective_end_date = %s,
                        is_current = FALSE
                    WHERE customer_key = %s
                """, (TODAY, customer_key))

                # Step B: Insert new version
                cursor.execute("""
                    SELECT MAX(record_version)
                    FROM dim_customer
                    WHERE customer_id = %s
                """, (row['customer_id'],))
                max_version = cursor.fetchone()[0] or 0

                cursor.execute("""
                    INSERT INTO dim_customer (
                        customer_id, customer_unique_id,
                        customer_city, customer_state,
                        customer_zip_prefix,
                        effective_start_date, effective_end_date,
                        is_current, record_version, etl_load_date
                    ) VALUES (%s, %s, %s, %s, %s, %s, NULL, TRUE, %s, %s)
                """, (
                    row['customer_id'],
                    row.get('customer_unique_id', row['customer_id']),
                    row['customer_city'],
                    row['customer_state'],
                    str(row.get('customer_zip_code_prefix', '')),
                    TODAY, max_version + 1, TODAY
                ))
                updated_count += 1

            else:
                # ── CASE 2: No change → skip ──────────────────
                skipped_count += 1

    conn.commit()

    print(f"  New customers inserted  : {new_count:>6,}")
    print(f"  Customers updated (SCD2): {updated_count:>6,}")
    print(f"  Customers unchanged     : {skipped_count:>6,}")

    cursor.execute("SELECT COUNT(*) FROM dim_customer")
    total = cursor.fetchone()[0]
    print(f"  Total records in dim_customer: {total:,}")

    log_etl_run('dim_customer', new_count + updated_count, 0, 'SUCCESS')

except Exception as e:
    print(f"  ERROR loading dim_customer: {e}")
    log_etl_run('dim_customer', 0, 0, 'FAILED', str(e))
    conn.rollback()
    raise


# ============================================================
# VERIFICATION QUERIES
# ── Always verify after loading — never assume it worked
# ============================================================
print("\n" + "=" * 60)
print("VERIFICATION CHECKS")
print("=" * 60)

verifications = [
    ("dim_date",     "SELECT COUNT(*) FROM dim_date"),
    ("dim_seller",   "SELECT COUNT(*) FROM dim_seller"),
    ("dim_product",  "SELECT COUNT(*) FROM dim_product"),
    ("dim_customer", "SELECT COUNT(*) FROM dim_customer WHERE is_current = TRUE"),
]

for table, query in verifications:
    cursor.execute(query)
    count = cursor.fetchone()[0]
    print(f"  {table:20} --> {count:>8,} records loaded")

# Date range check
cursor.execute("""
    SELECT MIN(full_date), MAX(full_date)
    FROM dim_date
""")
min_d, max_d = cursor.fetchone()
print(f"\n  dim_date covers: {min_d} to {max_d}")

# Product category check
cursor.execute("""
    SELECT price_band, COUNT(*) as cnt
    FROM dim_product
    GROUP BY price_band
    ORDER BY cnt DESC
""")
print("\n  Product price band distribution:")
for row in cursor.fetchall():
    print(f"    {str(row[0]):15} --> {row[1]:,}")


# ============================================================
# CLOSE CONNECTION
# ============================================================
cursor.close()
conn.close()

print("\n" + "=" * 60)
print("  PHASE 3 COMPLETE")
print("  All dimension tables loaded successfully")
print("  Next: Phase 4 - Load FACT_SALES table")
print("=" * 60)