# Retail Sales Analytics Pipeline

### End-to-End ETL | Star Schema Data Warehouse | Power BI Dashboard

![Executive Summary](dashboard/screenshots/page1_executive_summary.png)

---

## Project Overview

Built a production-grade data analytics pipeline for a Brazilian
e-commerce business covering 2 years of transactional data
(Sep 2016 – Oct 2018). The pipeline ingests raw source data,
applies enterprise ETL patterns, loads a MySQL star schema data
warehouse, and delivers executive-level insights through a
3-page Power BI dashboard.

| Item | Details |
|---|---|
| Dataset | Olist Brazilian E-Commerce (Kaggle) |
| Source Files | 8 CSV files (~400,000 raw records) |
| Warehouse | MySQL 8.0 (Star Schema) |
| Fact Records Loaded | 112,650 order line items |
| Total Revenue Analysed | R$ 13.59 Million |
| Date Range | September 2016 – October 2018 |

---

## Business Problem

A mid-size e-commerce company operating across 27 Brazilian
states had transactional data spread across multiple source
systems with no consolidated reporting layer. Business
leadership could not answer:

- Which product categories drive the most revenue?
- Are we delivering orders on time as the business scales?
- Which customers are Champions vs At Risk of churning?
- How is revenue trending month over month and year over year?
- Which states and sellers are underperforming?

**Solution:** Centralized data warehouse with automated ETL
pipelines and an executive Power BI dashboard refreshable daily.

---

## Architecture

```
Raw Source Files (8 CSVs)
         │
         ▼
┌─────────────────────┐
│  Phase 1            │
│  Data Profiling     │  Null audit, duplicate check,
│  & Quality Audit    │  referential integrity, date logic
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│  Phase 2            │
│  Staging &          │  Cleaning, type conversion,
│  Transformations    │  derived fields, reject routing
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│  Phase 3            │
│  Dimension Tables   │  SCD Type 2 (customers),
│  Load               │  surrogate keys, ETL control log
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│  Phase 4            │
│  Fact Table Load    │  Incremental watermark strategy,
│                     │  batch inserts, lookup resolution
└─────────────────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│  MySQL Star Schema  (retail_dw)         │
│                                         │
│  dim_date ──────────────────────────┐  │
│  dim_customer ── fact_sales ────────┤  │
│  dim_product  ──────────────────────┤  │
│  dim_seller   ──────────────────────┘  │
└─────────────────────────────────────────┘
         │
         ▼
┌─────────────────────┐
│  Phase 5            │
│  Advanced SQL       │  9 analytics queries using CTEs,
│  Analytics          │  window functions, cohort analysis
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│  Phase 6            │
│  Power BI           │  3-page executive dashboard,
│  Dashboard          │  15+ DAX measures, live MySQL
└─────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Profiling & ETL | Python 3.12 (Pandas, NumPy) |
| Data Warehouse | MySQL 8.0 |
| SQL Analytics | MySQL (CTEs, Window Functions) |
| Visualization | Power BI Desktop |
| Version Control | Git + GitHub |
| IDE | VS Code |

---

## Data Model — Star Schema

```
                     ┌──────────────┐
                     │   dim_date   │
                     │  1,461 rows  │
                     └──────┬───────┘
                            │
┌───────────────┐    ┌──────┴───────┐    ┌────────────────┐
│ dim_customer  │    │  fact_sales  │    │  dim_product   │
│  99,441 rows  ├────┤ 112,650 rows ├────┤  32,951 rows   │
└───────────────┘    └──────┬───────┘    └────────────────┘
                            │
                     ┌──────┴───────┐
                     │  dim_seller  │
                     │  3,095 rows  │
                     └──────────────┘
```

### Table Descriptions

| Table | Rows | Description |
|---|---|---|
| fact_sales | 112,650 | One row per order line item. Contains all measures: revenue, freight, delivery days, on-time flag |
| dim_date | 1,461 | Full date dimension 2016–2019. Supports all time-based analysis |
| dim_customer | 99,441 | Customer dimension with SCD Type 2 tracking |
| dim_product | 32,951 | Product dimension with English categories and price bands |
| dim_seller | 3,095 | Seller/vendor dimension with city and state |

---

## ETL Pipeline Features

### Phase 1 — Data Profiling

```python
# Example: Referential integrity check
orphan_items = items[~items['order_id'].isin(orders['order_id'])]
print(f"Items with no matching order: {len(orphan_items):,}")
# Result: 0 — clean dataset confirmed
```

- Null audit across all 8 source files with percentage reporting
- Duplicate detection on all primary key columns
- Referential integrity check across all table joins
- Date logic validation (delivery before purchase detection)
- Price sanity checks (zero/negative price detection)
- Profiling summary saved to `logs/profiling_summary.csv`

### Phase 2 — Staging & Transformations

```python
# Derived field example: delivery performance
df_orders['delivery_days'] = (
    df_orders['order_delivered_customer_date'] -
    df_orders['order_purchase_timestamp']
).dt.days

df_orders['is_on_time'] = np.where(
    df_orders['order_delivered_customer_date'] <=
    df_orders['order_estimated_delivery_date'], 1, 0
)
```

- Date column type conversion (string → datetime)
- Derived fields: date_key, delivery_days, is_on_time,
  delivery_delay_days, order_year, order_month, order_quarter
- English category name join from translation table
- Price band derivation: Budget / Mid-Range / Premium / Luxury
- Payment aggregation (multi-payment orders → 1 row per order)
- Full reject routing with timestamped audit log

### Phase 3 — SCD Type 2 Customer Dimension

```python
# SCD Type 2 logic - mirrors Informatica SCD mapping
if existing is None:
    # Case 1: New customer → INSERT
    cursor.execute("INSERT INTO dim_customer ...")

elif city_changed or state_changed:
    # Case 2: Changed → expire old record, insert new version
    cursor.execute("UPDATE dim_customer SET is_current=FALSE ...")
    cursor.execute("INSERT INTO dim_customer ...")

else:
    # Case 3: No change → skip
    skipped_count += 1
```

- SCD Type 2 on dim_customer tracks historical city/state changes
- Surrogate key auto-generation on all dimensions
- Idempotent dim_date load (skips if already populated)
- ETL control log records every run with status and row counts

### Phase 4 — Fact Table Load

```python
# High-watermark incremental strategy
def get_last_watermark():
    cursor.execute("""
        SELECT MAX(last_loaded_ts) FROM etl_control_log
        WHERE target_table = 'fact_sales'
        AND status = 'SUCCESS'
    """)
    return cursor.fetchone()[0]
```

- Dimension key resolution via in-memory lookup maps (O(1) lookup)
- Batch insert at 5,000 records per batch
- Full load vs incremental load auto-detection
- Zero lookup failures across all 112,650 records
- ETL control log updated with row counts after every run

---

## SQL Analytics

9 production-grade queries built on the star schema:

### Sample — RFM Customer Segmentation

```sql
WITH rfm_scored AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY recency_days ASC)  AS r_score,
        NTILE(5) OVER (ORDER BY frequency    DESC) AS f_score,
        NTILE(5) OVER (ORDER BY monetary     DESC) AS m_score
    FROM rfm_base
)
SELECT
    customer_segment,
    COUNT(*)                   AS customer_count,
    ROUND(AVG(monetary), 2)    AS avg_monetary,
    ROUND(SUM(monetary), 2)    AS total_revenue
FROM rfm_segmented
GROUP BY customer_segment
ORDER BY total_revenue DESC;
```

### Sample — Monthly Revenue with MoM Growth

```sql
SELECT
    year_month,
    revenue,
    LAG(revenue) OVER (ORDER BY year, month_num) AS prev_month,
    ROUND(
        (revenue - LAG(revenue) OVER (ORDER BY year, month_num))
        / NULLIF(LAG(revenue) OVER (ORDER BY year, month_num),0)
        * 100
    , 2) AS mom_growth_pct
FROM monthly_revenue
ORDER BY year, month_num;
```

| Query | Window Functions | Business Value |
|---|---|---|
| Monthly Revenue Trend | LAG(), SUM OVER (YTD) | Revenue momentum |
| YoY Comparison | CASE WHEN pivot | Year benchmarking |
| RFM Segmentation | NTILE(), chained CTEs | Customer intelligence |
| Product Ranking | RANK(), DENSE_RANK(), PARTITION BY | Category management |
| Seller Performance | RANK(), NTILE() quartiles | Vendor management |
| Cohort Retention | TIMESTAMPDIFF, chained CTEs | Customer lifecycle |
| Delivery Analysis | CASE WHEN buckets | Operational KPIs |
| Pareto 80/20 | Cumulative SUM OVER | Executive prioritization |
| Quarterly KPI | QoQ LAG, multi-metric | Board reporting |

---

## Power BI Dashboard

Three-page executive dashboard with live MySQL connection
and 15+ DAX measures.

### Page 1 — Executive Summary
![Executive Summary](dashboard/screenshots/page1_executive_summary.png)

KPI cards: Total Revenue (R$13.59M), Total Orders (99K),
Unique Customers (99K), Avg Order Value (R$137.75),
On-Time Delivery % (90.09%)

Visuals: Monthly revenue & order trend line chart,
Top 10 product categories bar chart,
Revenue by payment type donut chart,
Year and month slicers

### Page 2 — Customer Intelligence
![Customer Intelligence](dashboard/screenshots/page2_customer_intelligence.png)

Revenue by customer state (SP dominates at ~45%),
City-level revenue treemap,
Order status breakdown (97.28% completed),
State × metrics detail table with conditional formatting

### Page 3 — Product & Seller Performance
![Product & Seller Performance](dashboard/screenshots/page3_product_seller.png)

Product category treemap, Revenue by price band per year,
Delivery performance by seller state,
Freight cost % trend (rose from 6% to 18% as volume scaled)

---

## Key Business Insights

> **Insight 1 — Category Leadership**
> Health Beauty is the #1 revenue category at R$1.26M
> across 8,836 orders, followed by Watches Gifts and
> Bed Bath Table.

> **Insight 2 — Geographic Concentration Risk**
> São Paulo accounts for ~45% of total revenue.
> Top 3 states (SP, RJ, MG) contribute ~65% of revenue —
> geographic over-dependence identified.

> **Insight 3 — Payment Infrastructure**
> Credit card dominates at 78.91% of revenue.
> Boleto (Brazilian bank slip) accounts for 17.6% —
> important for market-specific payment strategy.

> **Insight 4 — Logistics Cost Pressure**
> Freight cost rose from 6.29% to 17.98% of revenue
> as order volume scaled 80x from Q4 2016 to Q1 2018.
> Signals need for logistics renegotiation or regional
> warehousing investment.

> **Insight 5 — Delivery Quality at Scale**
> On-time delivery degraded from 99.04% (266 orders in Q4 2016)
> to 85.79% (20,627 orders in Q1 2018).
> Fulfillment infrastructure did not scale proportionally
> with order growth.

> **Insight 6 — Stable Pricing**
> Average order value remained consistent at R$114–128
> across all quarters — pricing strategy is stable and
> not being discounted to drive volume.

---

## ETL Run Results

```
Table            Status    Records    Run Time
─────────────────────────────────────────────
dim_date         SUCCESS     1,461    Phase 3
dim_seller       SUCCESS     3,095    Phase 3
dim_product      SUCCESS    32,951    Phase 3
dim_customer     SUCCESS    99,441    Phase 3
fact_sales       SUCCESS   112,650    Phase 4
─────────────────────────────────────────────
Total loaded               249,588
Rejected records                 0
Lookup failures                  0
```

---

## Project Structure

```
retail-sales-analytics/
│
├── data/
│   ├── raw/                         ← Download from Kaggle (see below)
│   └── staging/                     ← Generated by ETL pipeline
│       ├── stg_orders.csv
│       ├── stg_items.csv
│       ├── stg_customers.csv
│       ├── stg_products.csv
│       └── stg_payments.csv
│
├── etl/
│   ├── utils/
│   │   └── db_connector.py          ← MySQL connection utility
│   ├── 01_data_profiling.py         ← Phase 1: Quality audit
│   ├── 02_staging_transforms.py     ← Phase 2: Clean & transform
│   ├── 03_load_dimensions.py        ← Phase 3: Load dims (SCD2)
│   └── 04_load_fact.py              ← Phase 4: Load fact table
│
├── sql/
│   ├── ddl/
│   │   └── 01_create_schema.sql     ← Star schema DDL
│   └── analytics/
│       └── 01_analytics_queries.sql ← 9 advanced SQL queries
│
├── dashboard/
│   ├── retail_dashboard.pbix        ← Power BI file
│   └── screenshots/
│       ├── page1_executive_summary.png
│       ├── page2_customer_intelligence.png
│       └── page3_product_seller.png
│
├── logs/                            ← Generated by ETL pipeline
│   ├── profiling_summary.csv
│   ├── staging_summary.csv
│   └── reject_log_YYYYMMDD.csv
│
├── .gitignore
└── README.md
```

---

## How to Run

### Prerequisites
- Python 3.8+
- MySQL 8.0
- Power BI Desktop
- Kaggle account (for dataset download)

### Setup

```bash
# 1. Clone the repository
git clone https://github.com/yourusername/retail-sales-analytics.git
cd retail-sales-analytics

# 2. Create virtual environment
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux

# 3. Install dependencies
pip install pandas numpy mysql-connector-python sqlalchemy openpyxl

# 4. Download dataset
# Go to: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
# Download and extract all CSV files to data/raw/

# 5. Configure database connection
# Edit etl/utils/db_connector.py
# Update host, port, database, user, password

# 6. Create MySQL database and schema
# Open MySQL Workbench
# Run: CREATE DATABASE retail_dw;
# Run sql/ddl/01_create_schema.sql

# 7. Run ETL pipeline in sequence
python etl/01_data_profiling.py
python etl/02_staging_transforms.py
python etl/03_load_dimensions.py
python etl/04_load_fact.py

# 8. Run SQL analytics
# Open sql/analytics/01_analytics_queries.sql
# Execute in MySQL Workbench or VS Code SQLTools

# 9. Open Power BI dashboard
# Open dashboard/retail_dashboard.pbix
# Refresh data connection to your MySQL instance
```

---

## Author

**Manohar Marepally**
Data Analyst | ETL & Business Intelligence

6 years of IT experience in ETL (Informatica, Oracle PL/SQL)
transitioning into Data Analytics with expertise in Python,
SQL, Power BI, and data warehouse design.
