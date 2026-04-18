# ============================================================
# 02_staging_transforms.py
# Project : Retail Sales Analytics Pipeline
# Purpose : Clean, transform and prepare all source data
#           for warehouse loading
#
# Informatica Mapping Equivalent:
#   Source Qualifier → Expression → Router → Target
#
# Author  : Manohar Marepally
# ============================================================

import pandas as pd
import numpy as np
import os
from datetime import datetime

# ── Path Configuration ────────────────────────────────────────
RAW_PATH     = 'data/raw/'
STAGING_PATH = 'data/staging/'
LOG_PATH     = 'logs/'

os.makedirs(STAGING_PATH, exist_ok=True)
os.makedirs(LOG_PATH,     exist_ok=True)

run_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
reject_log    = []   # collects all rejected records across all transforms

print("=" * 60)
print("PHASE 2: STAGING & TRANSFORMATIONS")
print(f"Run started at: {run_timestamp}")
print("=" * 60)


# ============================================================
# LOAD RAW SOURCE FILES
# ── Same as Informatica Source Qualifier reading flat files
# ============================================================
print("\nLoading raw source files...")

orders    = pd.read_csv(RAW_PATH + 'olist_orders_dataset.csv')
items     = pd.read_csv(RAW_PATH + 'olist_order_items_dataset.csv')
payments  = pd.read_csv(RAW_PATH + 'olist_order_payments_dataset.csv')
customers = pd.read_csv(RAW_PATH + 'olist_customers_dataset.csv')
products  = pd.read_csv(RAW_PATH + 'olist_products_dataset.csv')
sellers   = pd.read_csv(RAW_PATH + 'olist_sellers_dataset.csv')
cat_trans = pd.read_csv(RAW_PATH + 'product_category_name_translation.csv')

print("All files loaded.\n")


# ============================================================
# TRANSFORM 1: CLEAN ORDERS
# ── Convert date columns (string → datetime)
# ── Flag and reject invalid records
# ── Derive new business fields
#
# Informatica Equivalent:
#   Expression transformation for date conversions
#   Router transformation to split valid vs rejected
# ============================================================
print("=" * 60)
print("TRANSFORM 1: CLEANING ORDERS TABLE")
print("=" * 60)

df_orders = orders.copy()

# Step 1: Convert all timestamp columns to datetime
date_cols = [
    'order_purchase_timestamp',
    'order_approved_at',
    'order_delivered_carrier_date',
    'order_delivered_customer_date',
    'order_estimated_delivery_date'
]

for col in date_cols:
    df_orders[col] = pd.to_datetime(df_orders[col], errors='coerce')

print(f"  Date columns converted to datetime")

# Step 2: Reject records with null purchase timestamp
# (this is the most critical field — without it we can't
#  build the date key for the fact table)
null_purchase = df_orders[df_orders['order_purchase_timestamp'].isnull()].copy()
null_purchase['reject_reason'] = 'NULL purchase timestamp'
reject_log.append(null_purchase)

df_orders = df_orders[df_orders['order_purchase_timestamp'].notnull()]
print(f"  Rejected {len(null_purchase):,} records with null purchase timestamp")

# Step 3: Reject records with invalid date logic
# Delivery date cannot be before purchase date
invalid_date_logic = df_orders[
    df_orders['order_delivered_customer_date'].notnull() &
    (df_orders['order_delivered_customer_date'] < df_orders['order_purchase_timestamp'])
].copy()
invalid_date_logic['reject_reason'] = 'Delivery date before purchase date'
reject_log.append(invalid_date_logic)

df_orders = df_orders[
    ~df_orders['order_id'].isin(invalid_date_logic['order_id'])
]
print(f"  Rejected {len(invalid_date_logic):,} records with invalid date logic")

# Step 4: Derive new fields
# (Expression transformation logic in Informatica)

# Date key for warehouse join: YYYYMMDD integer format
df_orders['date_key'] = (
    df_orders['order_purchase_timestamp']
    .dt.strftime('%Y%m%d')
    .astype(int)
)

# Year, Month, Quarter for easy grouping
df_orders['order_year']    = df_orders['order_purchase_timestamp'].dt.year
df_orders['order_month']   = df_orders['order_purchase_timestamp'].dt.month
df_orders['order_quarter'] = df_orders['order_purchase_timestamp'].dt.quarter

# Delivery lead time in days
df_orders['delivery_days'] = (
    df_orders['order_delivered_customer_date'] -
    df_orders['order_purchase_timestamp']
).dt.days

# Was the order delivered on time?
df_orders['is_on_time'] = np.where(
    df_orders['order_delivered_customer_date'].notnull() &
    (
        df_orders['order_delivered_customer_date'] <=
        df_orders['order_estimated_delivery_date']
    ),
    1, 0
)

# Delay in days (negative = early, positive = late)
df_orders['delivery_delay_days'] = (
    df_orders['order_delivered_customer_date'] -
    df_orders['order_estimated_delivery_date']
).dt.days

# Order status category (simplify for reporting)
df_orders['status_category'] = df_orders['order_status'].map({
    'delivered'  : 'Completed',
    'shipped'    : 'In Transit',
    'approved'   : 'Processing',
    'processing' : 'Processing',
    'invoiced'   : 'Processing',
    'canceled'   : 'Canceled',
    'unavailable': 'Canceled',
    'created'    : 'Processing'
})

print(f"  Derived fields added: date_key, delivery_days, is_on_time,")
print(f"  delivery_delay_days, order_year, order_month, order_quarter")
print(f"  Clean orders remaining: {len(df_orders):,}")


# ============================================================
# TRANSFORM 2: CLEAN ORDER ITEMS
# ── Validate price and freight values
# ── Calculate line-level revenue metrics
# ── Join with orders to inherit date_key
# ============================================================
print("\n" + "=" * 60)
print("TRANSFORM 2: CLEANING ORDER ITEMS")
print("=" * 60)

df_items = items.copy()

# Step 1: Reject zero or negative price records
invalid_price = df_items[df_items['price'] <= 0].copy()
invalid_price['reject_reason'] = 'Zero or negative price'
reject_log.append(invalid_price)

df_items = df_items[df_items['price'] > 0]
print(f"  Rejected {len(invalid_price):,} records with invalid price")

# Step 2: Fill null freight with 0
# Business decision: null freight = free shipping
null_freight = df_items['freight_value'].isnull().sum()
df_items['freight_value'] = df_items['freight_value'].fillna(0)
print(f"  Filled {null_freight:,} null freight values with 0")

# Step 3: Reject items with no matching order
# (Referential integrity — mirrors Informatica Lookup reject port)
orphan_items = df_items[
    ~df_items['order_id'].isin(df_orders['order_id'])
].copy()
orphan_items['reject_reason'] = 'No matching order in orders table'
reject_log.append(orphan_items)

df_items = df_items[df_items['order_id'].isin(df_orders['order_id'])]
print(f"  Rejected {len(orphan_items):,} orphan items with no matching order")

# Step 4: Derive revenue fields
df_items['gross_revenue'] = df_items['price']
df_items['total_cost']    = df_items['price'] + df_items['freight_value']

# Step 5: Convert shipping_limit_date to datetime
df_items['shipping_limit_date'] = pd.to_datetime(
    df_items['shipping_limit_date'], errors='coerce'
)

# Step 6: Join date_key from clean orders
df_items = df_items.merge(
    df_orders[['order_id', 'date_key', 'order_year',
               'order_month', 'order_quarter',
               'is_on_time', 'delivery_days',
               'status_category']],
    on='order_id',
    how='left'
)

print(f"  Derived fields: gross_revenue, total_cost")
print(f"  Joined date_key and delivery fields from orders")
print(f"  Clean items remaining: {len(df_items):,}")


# ============================================================
# TRANSFORM 3: CLEAN CUSTOMERS
# ── Standardize text fields
# ── Remove duplicates
# ── Will support SCD Type 2 in Phase 3
# ============================================================
print("\n" + "=" * 60)
print("TRANSFORM 3: CLEANING CUSTOMERS")
print("=" * 60)

df_customers = customers.copy()

# Step 1: Standardize text — strip whitespace, title case
df_customers['customer_city'] = (
    df_customers['customer_city']
    .str.strip()
    .str.title()
)
df_customers['customer_state'] = (
    df_customers['customer_state']
    .str.strip()
    .str.upper()
)

# Step 2: Remove duplicate customer_ids
before = len(df_customers)
df_customers = df_customers.drop_duplicates(subset=['customer_id'])
after = len(df_customers)
print(f"  Removed {before - after:,} duplicate customer records")

# Step 3: Add a load timestamp
df_customers['etl_load_date'] = pd.Timestamp.today().date()

print(f"  Standardized city and state text fields")
print(f"  Clean customers: {len(df_customers):,}")


# ============================================================
# TRANSFORM 4: CLEAN PRODUCTS
# ── Join English category names
# ── Derive price band and weight category
# ── Handle null category names
# ============================================================
print("\n" + "=" * 60)
print("TRANSFORM 4: CLEANING PRODUCTS")
print("=" * 60)

df_products = products.copy()

# Step 1: Join English category translation
# (Lookup transformation in Informatica)
df_products = df_products.merge(
    cat_trans,
    on='product_category_name',
    how='left'
)

# Step 2: Fill null category names
df_products['product_category_name_english'] = (
    df_products['product_category_name_english']
    .fillna('uncategorized')
    .str.strip()
    .str.replace('_', ' ')
    .str.title()
)

null_categories = df_products[
    df_products['product_category_name'].isnull()
].shape[0]
print(f"  Null category names defaulted to 'uncategorized': {null_categories:,}")

# Step 3: Derive price band
# We'll calculate avg price per product from items
avg_price_per_product = (
    items.groupby('product_id')['price']
    .mean()
    .reset_index()
    .rename(columns={'price': 'avg_price'})
)

df_products = df_products.merge(
    avg_price_per_product, on='product_id', how='left'
)

# Price band segmentation
df_products['price_band'] = pd.cut(
    df_products['avg_price'],
    bins=[0, 50, 200, 500, float('inf')],
    labels=['Budget', 'Mid-Range', 'Premium', 'Luxury'],
    right=True
)
df_products['price_band'] = df_products['price_band'].astype(str)
df_products['price_band'] = df_products['price_band'].replace('nan', 'Unknown')

# Step 4: Derive weight category
df_products['weight_category'] = pd.cut(
    df_products['product_weight_g'],
    bins=[0, 500, 2000, 10000, float('inf')],
    labels=['Light', 'Medium', 'Heavy', 'Bulky'],
    right=True
)
df_products['weight_category'] = df_products['weight_category'].astype(str)
df_products['weight_category'] = df_products['weight_category'].replace('nan', 'Unknown')

print(f"  Joined English category names")
print(f"  Derived: price_band, weight_category")
print(f"  Clean products: {len(df_products):,}")


# ============================================================
# TRANSFORM 5: CLEAN PAYMENTS
# ── Aggregate payment info per order
# ── Handle multiple payment rows per order
# ============================================================
print("\n" + "=" * 60)
print("TRANSFORM 5: CLEANING PAYMENTS")
print("=" * 60)

df_payments = payments.copy()

# One order can have multiple payment rows
# (e.g. part credit card, part voucher)
# Aggregate to one row per order for fact table joining

df_payments_agg = df_payments.groupby('order_id').agg(
    total_payment_value  = ('payment_value',        'sum'),
    payment_installments = ('payment_installments', 'max'),
    payment_types_used   = ('payment_type',         'nunique'),
    primary_payment_type = ('payment_type',          lambda x: x.value_counts().index[0])
).reset_index()

print(f"  Aggregated {len(df_payments):,} payment rows")
print(f"  Into {len(df_payments_agg):,} order-level payment records")
print(f"  Primary payment type preserved per order")


# ============================================================
# SAVE REJECT LOG
# ── All rejected records from all transforms saved together
# ── This is your audit trail (like Informatica's bad file)
# ============================================================
print("\n" + "=" * 60)
print("SAVING REJECT LOG")
print("=" * 60)

all_rejects = pd.concat(reject_log, ignore_index=True)
reject_filename = f"logs/reject_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
all_rejects.to_csv(reject_filename, index=False)
print(f"  Total records rejected across all transforms : {len(all_rejects):,}")
print(f"  Reject log saved --> {reject_filename}")


# ============================================================
# SAVE STAGING FILES
# ── Clean, transformed data written to data/staging/
# ── These become the source for Phase 3 warehouse load
# ── Equivalent to Informatica writing to staging targets
# ============================================================
print("\n" + "=" * 60)
print("SAVING STAGING FILES")
print("=" * 60)

df_orders.to_csv(   STAGING_PATH + 'stg_orders.csv',    index=False)
df_items.to_csv(    STAGING_PATH + 'stg_items.csv',     index=False)
df_customers.to_csv(STAGING_PATH + 'stg_customers.csv', index=False)
df_products.to_csv( STAGING_PATH + 'stg_products.csv',  index=False)
df_payments_agg.to_csv(STAGING_PATH + 'stg_payments.csv', index=False)

print(f"  stg_orders.csv    --> {len(df_orders):,} records")
print(f"  stg_items.csv     --> {len(df_items):,} records")
print(f"  stg_customers.csv --> {len(df_customers):,} records")
print(f"  stg_products.csv  --> {len(df_products):,} records")
print(f"  stg_payments.csv  --> {len(df_payments_agg):,} records")


# ============================================================
# STAGING SUMMARY REPORT
# ============================================================
print("\n" + "=" * 60)
print("PHASE 2 SUMMARY")
print("=" * 60)

summary = {
    'run_timestamp'         : run_timestamp,
    'stg_orders_rows'       : len(df_orders),
    'stg_items_rows'        : len(df_items),
    'stg_customers_rows'    : len(df_customers),
    'stg_products_rows'     : len(df_products),
    'stg_payments_rows'     : len(df_payments_agg),
    'total_records_rejected': len(all_rejects),
}

pd.DataFrame([summary]).to_csv(
    LOG_PATH + 'staging_summary.csv', index=False
)

print(f"\n  Staging summary saved --> logs/staging_summary.csv")
print("\n" + "=" * 60)
print("  PHASE 2 COMPLETE")
print("  Next: Phase 3 - Load Dimension Tables to MySQL")
print("=" * 60)