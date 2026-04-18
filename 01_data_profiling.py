# ============================================================
# 01_data_profiling.py
# Project : Retail Sales Analytics Pipeline
# Purpose : Profile all source files before any transformation
# Author  : Manohar
# ============================================================

import pandas as pd
import numpy as np
import os
from datetime import datetime

# ── Path Configuration ────────────────────────────────────────
RAW_PATH = 'data/raw/'
LOG_PATH = 'logs/'
os.makedirs(LOG_PATH, exist_ok=True)

# ============================================================
# LOAD ALL SOURCE FILES
# ============================================================
print("=" * 60)
print("LOADING SOURCE FILES")
print("=" * 60)

orders    = pd.read_csv(RAW_PATH + 'olist_orders_dataset.csv')
items     = pd.read_csv(RAW_PATH + 'olist_order_items_dataset.csv')
payments  = pd.read_csv(RAW_PATH + 'olist_order_payments_dataset.csv')
customers = pd.read_csv(RAW_PATH + 'olist_customers_dataset.csv')
products  = pd.read_csv(RAW_PATH + 'olist_products_dataset.csv')
sellers   = pd.read_csv(RAW_PATH + 'olist_sellers_dataset.csv')
cat_trans = pd.read_csv(RAW_PATH + 'product_category_name_translation.csv')

print("All files loaded successfully.\n")

datasets = {
    'orders'   : orders,
    'items'    : items,
    'payments' : payments,
    'customers': customers,
    'products' : products,
    'sellers'  : sellers
}

# ============================================================
# SECTION 1: SHAPE & VOLUME CHECK
# ============================================================
print("=" * 60)
print("SECTION 1: SHAPE & VOLUME CHECK")
print("=" * 60)

for name, df in datasets.items():
    print(f"  {name:15} --> {df.shape[0]:>7,} rows  | {df.shape[1]:>3} columns")

# ============================================================
# SECTION 2: NULL VALUE AUDIT
# ============================================================
print("\n" + "=" * 60)
print("SECTION 2: NULL VALUE AUDIT")
print("=" * 60)

def null_audit(df, name):
    total = len(df)
    nulls = df.isnull().sum()
    nulls = nulls[nulls > 0]
    if nulls.empty:
        print(f"\n  [{name}] No nulls found - Clean")
    else:
        print(f"\n  [{name}] Nulls detected:")
        for col, count in nulls.items():
            pct = round(count / total * 100, 2)
            print(f"    {col:45} --> {count:>5,} nulls ({pct}%)")

for name, df in datasets.items():
    null_audit(df, name)

# ============================================================
# SECTION 3: DUPLICATE CHECK
# ============================================================
print("\n" + "=" * 60)
print("SECTION 3: DUPLICATE CHECK ON PRIMARY KEYS")
print("=" * 60)

pk_checks = [
    (orders,    'order_id',    'orders'),
    (customers, 'customer_id', 'customers'),
    (products,  'product_id',  'products'),
    (sellers,   'seller_id',   'sellers'),
]

for df, key_col, name in pk_checks:
    dupes = df.duplicated(subset=[key_col]).sum()
    if dupes == 0:
        print(f"  [{name}] {key_col:30} --> No duplicates - Clean")
    else:
        print(f"  [{name}] {key_col:30} --> {dupes:,} DUPLICATES FOUND")

# ============================================================
# SECTION 4: DATA TYPE INSPECTION
# ============================================================
print("\n" + "=" * 60)
print("SECTION 4: DATA TYPE INSPECTION")
print("=" * 60)

print("\n  [orders] Column Data Types:")
for col, dtype in orders.dtypes.items():
    print(f"    {col:45} --> {dtype}")

# ============================================================
# SECTION 5: DATE LOGIC VALIDATION
# ============================================================
print("\n" + "=" * 60)
print("SECTION 5: DATE LOGIC VALIDATION")
print("=" * 60)

orders['purchase_ts']  = pd.to_datetime(
    orders['order_purchase_timestamp'], errors='coerce'
)
orders['approved_ts']  = pd.to_datetime(
    orders['order_approved_at'], errors='coerce'
)
orders['delivered_ts'] = pd.to_datetime(
    orders['order_delivered_customer_date'], errors='coerce'
)
orders['estimated_ts'] = pd.to_datetime(
    orders['order_estimated_delivery_date'], errors='coerce'
)

# Rule 1: Delivery before purchase (data error)
invalid_delivery = orders[
    orders['delivered_ts'].notnull() &
    (orders['delivered_ts'] < orders['purchase_ts'])
]
print(f"\n  Orders where delivery < purchase date : {len(invalid_delivery):>5,}")

# Rule 2: Future purchase timestamps
future_orders = orders[orders['purchase_ts'] > pd.Timestamp.now()]
print(f"  Orders with future purchase timestamp : {len(future_orders):>5,}")

# Rule 3: Unparseable dates
bad_dates = orders[orders['purchase_ts'].isnull()]
print(f"  Orders with unparseable purchase date : {len(bad_dates):>5,}")

# Rule 4: Approval before purchase
invalid_approval = orders[
    orders['approved_ts'].notnull() &
    (orders['approved_ts'] < orders['purchase_ts'])
]
print(f"  Orders where approval < purchase date : {len(invalid_approval):>5,}")

# ============================================================
# SECTION 6: ORDER STATUS DISTRIBUTION
# ============================================================
print("\n" + "=" * 60)
print("SECTION 6: VALUE DISTRIBUTION")
print("=" * 60)

print("\n  Order Status Distribution:")
status_dist = orders['order_status'].value_counts()
total_orders = len(orders)
for status, count in status_dist.items():
    pct = round(count / total_orders * 100, 2)
    print(f"    {status:20} --> {count:>6,}  ({pct}%)")

print("\n  Payment Type Distribution:")
pay_dist = payments['payment_type'].value_counts()
for ptype, count in pay_dist.items():
    pct = round(count / len(payments) * 100, 2)
    print(f"    {ptype:20} --> {count:>6,}  ({pct}%)")

# ============================================================
# SECTION 7: PRICE & REVENUE SANITY CHECK
# ============================================================
print("\n" + "=" * 60)
print("SECTION 7: PRICE SANITY CHECK")
print("=" * 60)

print("\n  Price column statistics:")
print(items['price'].describe().round(2).to_string())

zero_price    = (items['price'] <= 0).sum()
zero_freight  = (items['freight_value'] == 0).sum()
mean_p        = items['price'].mean()
std_p         = items['price'].std()
outliers      = items[items['price'] > mean_p + 3 * std_p]

print(f"\n  Zero or negative price records : {zero_price:>5,}")
print(f"  Zero freight records           : {zero_freight:>5,}")
print(f"  Price outliers (> 3 std dev)   : {len(outliers):>5,}")
print(f"  Highest price item             : R$ {items['price'].max():>10,.2f}")
print(f"  Average price                  : R$ {items['price'].mean():>10,.2f}")

# ============================================================
# SECTION 8: REFERENTIAL INTEGRITY CHECK
# ============================================================
print("\n" + "=" * 60)
print("SECTION 8: REFERENTIAL INTEGRITY CHECK")
print("=" * 60)

orphan_items = items[~items['order_id'].isin(orders['order_id'])]
orders_no_customer = orders[~orders['customer_id'].isin(customers['customer_id'])]
items_no_product   = items[~items['product_id'].isin(products['product_id'])]
items_no_seller    = items[~items['seller_id'].isin(sellers['seller_id'])]

print(f"\n  Items with no matching order         : {len(orphan_items):>5,}")
print(f"  Orders with no matching customer     : {len(orders_no_customer):>5,}")
print(f"  Items with no matching product       : {len(items_no_product):>5,}")
print(f"  Items with no matching seller        : {len(items_no_seller):>5,}")

if len(orphan_items) > 0:
    orphan_items.to_csv(LOG_PATH + 'orphan_items.csv', index=False)
    print("  Orphan items saved --> logs/orphan_items.csv")

# ============================================================
# SECTION 9: DATE RANGE CHECK
# ============================================================
print("\n" + "=" * 60)
print("SECTION 9: DATA DATE RANGE")
print("=" * 60)

print(f"\n  Orders date range:")
print(f"    Earliest purchase : {orders['purchase_ts'].min()}")
print(f"    Latest purchase   : {orders['purchase_ts'].max()}")
print(f"    Total date span   : {(orders['purchase_ts'].max() - orders['purchase_ts'].min()).days} days")

# ============================================================
# SECTION 10: SAVE PROFILING SUMMARY TO LOG
# ============================================================
print("\n" + "=" * 60)
print("SECTION 10: SAVING PROFILING SUMMARY LOG")
print("=" * 60)

summary = {
    'profiled_at'             : datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    'orders_rows'             : len(orders),
    'items_rows'              : len(items),
    'customers_rows'          : len(customers),
    'products_rows'           : len(products),
    'sellers_rows'            : len(sellers),
    'invalid_delivery_dates'  : len(invalid_delivery),
    'future_orders'           : len(future_orders),
    'orphan_items'            : len(orphan_items),
    'zero_price_items'        : int(zero_price),
    'price_outliers'          : len(outliers),
    'orders_no_customer'      : len(orders_no_customer),
}

pd.DataFrame([summary]).to_csv(
    LOG_PATH + 'profiling_summary.csv', index=False
)

print("\n  Profiling summary saved --> logs/profiling_summary.csv")
print("\n" + "=" * 60)
print("  PHASE 1 COMPLETE - Review findings before Phase 2")
print("=" * 60)