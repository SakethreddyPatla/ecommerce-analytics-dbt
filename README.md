# 📊 E-Commerce Analytics Data Warehouse

[![dbt](https://img.shields.io/badge/dbt-1.11-orange)](https://www.getdbt.com/)
[![Python](https://img.shields.io/badge/python-3.8+-blue)](https://www.python.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-latest-yellow)](https://duckdb.org/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-yellow)](https://powerbi.microsoft.com/)

> Complete end-to-end analytics engineering project analyzing 100k+ e-commerce orders. Built with real messy data, demonstrating dimensional modeling, dbt transformations, automated testing, and interactive dashboards.

---

## Project Overview

This project showcases a **production-ready data warehouse** built from scratch using modern analytics engineering practices. Starting with raw Brazilian e-commerce data (Olist dataset), I designed and implemented a complete star schema, handled data quality issues, and created interactive dashboards answering key business questions.

### Key Achievements
-  **112,101 order items** processed and analyzed
-  **99,441 customers** segmented and profiled
-  **40 automated tests** - 100% passing
-  **92.1% on-time delivery** performance tracked
-  **Star schema** with 4 dimensions + 1 fact table

---

## Architecture
```
Raw CSV Data (9 files, 100k+ orders)
    ↓
DuckDB Data Warehouse
    ↓
dbt Transformations
    ├── Staging Layer (data cleaning)
    ├── Dimension Tables (customer, product, seller, date)
    ├── Fact Table (order items)
    └── Metrics Layer (aggregations)
    ↓
Power BI Dashboard (interactive visualizations)
```

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Storage** | DuckDB | Embedded analytical database |
| **Transformation** | dbt | SQL-based data transformations |
| **Testing** | dbt tests | Automated data quality checks |
| **Language** | SQL, Python | Data processing and scripting |
| **Visualization** | Power BI | Interactive dashboards |
| **Version Control** | Git, GitHub | Code management |

---

## Data Model

### Star Schema Design

**Fact Table:**
- `fct_order_items` - 112,101 rows (grain: one order line item)

**Dimension Tables:**
- `dim_customers` - 99,441 rows with behavioral segmentation
- `dim_products` - 32,951 rows with English translations
- `dim_sellers` - 3,095 rows with performance tiers
- `dim_dates` - 1,461 rows (calendar table 2016-2019)

### Key Design Decisions

**Customer Segmentation:**
```sql
CASE 
    WHEN lifetime_orders = 0 THEN 'New'
    WHEN lifetime_orders = 1 THEN 'One-time'
    WHEN lifetime_orders BETWEEN 2 AND 5 THEN 'Regular'
    WHEN lifetime_orders > 5 THEN 'VIP'
END
```

**Data Quality Handling:**
-  Excluded 775 unavailable/canceled orders
-  Replaced NULL categories with "Uncategorized"
-  Calculated on-time delivery metrics
-  Validated referential integrity across all tables

---

## Dashboard Features

### Page 1: Executive Dashboard
- Monthly revenue trends with 7-day moving average
- KPI cards (revenue, orders, customers, avg order value)
- Revenue by product category
- Top cities by revenue
- Customer segment distribution
- Interactive date filtering

### Page 2: Product & Seller Analysis
- Top product categories by performance
- Top 10 sellers with performance tiers
- Delivery performance metrics (92.1% on-time!)
- Seller tier distribution

---

## Getting Started

### Prerequisites
- Python 3.8+
- Git

### Installation
```bash
# Clone the repository
git clone https://github.com/SakethreddyPatla/ecommerce-analytics-dbt.git
cd ecommerce-analytics-dbt

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install dbt-duckdb pandas

# Download Olist dataset
# Visit: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
# Extract to: olist_data/
```

### Running the Pipeline
```bash
# 1. Load raw data
python load_to_database.py

# 2. Run dbt transformations
cd ecommerce_analytics
dbt run

# 3. Run data quality tests
dbt test

# 4. View dbt documentation
dbt docs generate
dbt docs serve
```

Expected output: **40 tests passing**

---

## Data Quality Testing

### Test Coverage (40 tests)

**Uniqueness Tests:**
- Primary keys in all dimension tables
- Date uniqueness in calendar dimension

**Not Null Tests:**
- Critical fields (IDs, dates, status)
- Foreign keys in fact table

**Referential Integrity:**
- All fact table foreign keys validate against dimensions
- Customer, product, seller, and date relationships

**Business Logic:**
- Order status contains only valid values
- Customer segments follow defined categories
- Seller performance tiers are properly classified

**Run tests:**
```bash
cd ecommerce_analytics
dbt test
```

---

## Business Questions Answered

1.  **Monthly revenue trends** - Line chart with moving averages
2.  **Top product categories** - Health & Beauty leads at $1.4M
3.  **Geographic distribution** - São Paulo dominates with $2.15M
4.  **Best-selling products** - Bed/Bath/Table most popular (11k items)
5.  **Average order value** - $140.37 per order
6.  **Top sellers** - 89 "Top Performer" tier sellers identified
7.  **Seller-product relationships** - Drill-down capabilities
8.  **Customer segments** - 98k one-time buyers (conversion opportunity!)
9.  **Delivery performance** - 92.1% on-time delivery rate
10. **Seller speed** - Performance tiers show delivery patterns

---

## Project Structure
```
ecommerce-analytics-dbt/
├── ecommerce_analytics/          # dbt project
│   ├── models/
│   │   ├── staging/             # Data cleaning layer
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_products.sql
│   │   │   ├── stg_orders.sql
│   │   │   └── stg_order_items.sql
│   │   │
│   │   ├── marts/               # Business logic layer
│   │   │   ├── dim_customers.sql
│   │   │   ├── dim_products.sql
│   │   │   ├── dim_sellers.sql
│   │   │   ├── dim_dates.sql
│   │   │   └── fct_order_items.sql
│   │   │
│   │   └── schema.yml           # Tests & documentation
│   │
│   ├── dbt_project.yml
│   └── profiles.yml
│
├── screenshots/                  # Dashboard images
├── data_quality_checks.py       # Quality analysis script
├── export_for_powerbi.py        # Dashboard export script
├── .gitignore
└── README.md
```

---

## Key Learnings

### Technical Skills Demonstrated

**SQL:**
- Complex CTEs and window functions
- Star schema dimensional modeling
- Aggregate functions and GROUP BY
- JOINs across multiple tables
- CASE statements for business logic

**dbt:**
- Model organization (staging → marts)
- Ref() function for dependencies
- Schema testing (uniqueness, not_null, relationships)
- Documentation with YAML
- Best practices (CTEs, modularity)

**Data Quality:**
- Root cause analysis of data issues
- Handling NULL values and missing data
- Excluding invalid records (canceled/unavailable)
- Referential integrity validation
- Automated testing

**Analytics Engineering:**
- Dimensional modeling (Kimball methodology)
- Star schema implementation
- Slowly changing dimensions concepts
- Fact and dimension design patterns
- Performance optimization through denormalization

### Real-World Challenges Solved

1. **775 orders without items** - Investigated and found 78% were "unavailable" orders (stock-outs)
2. **610 products with NULL categories** - Decided to label as "Uncategorized" for analysis
3. **8 delivered orders without dates** - Flagged as data errors, excluded from delivery metrics
4. **Performance optimization** - Pre-calculated customer segments and seller tiers in dimensions

---

## Future Enhancements

- [ ] Implement incremental models for large fact tables
- [ ] Add slowly changing dimensions (SCD Type 2) for historical tracking
- [ ] Create customer lifetime value (CLV) predictions
- [ ] Add more advanced metrics (cohort retention, churn analysis)
- [ ] Deploy to cloud (Snowflake/BigQuery)
- [ ] Set up CI/CD with GitHub Actions
- [ ] Implement data observability with Great Expectations
- [ ] Add ML models for demand forecasting

---
