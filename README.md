# E-Commerce Analytics Data Warehouse

[![dbt](https://img.shields.io/badge/dbt-1.11-orange)](https://www.getdbt.com/)
[![Python](https://img.shields.io/badge/python-3.8+-blue)](https://www.python.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-latest-yellow)](https://duckdb.org/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-yellow)](https://powerbi.microsoft.com/)

> Complete end-to-end analytics engineering project analyzing 100k+ e-commerce orders. Built with real messy data, demonstrating dimensional modeling, dbt transformations, automated testing, and interactive dashboards.

---

## Project Overview

This project showcases a **production-ready data warehouse** built from scratch using modern analytics engineering practices. Starting with raw Brazilian e-commerce data (Olist dataset), I designed and implemented a complete star schema, handled data quality issues, and created interactive dashboards answering key business questions.

### Key Achievements
- **112,101 order items** processed and analyzed
- **99,441 customers** segmented and profiled
- **40 automated data quality tests** - 100% passing
- **92.1% on-time delivery** performance tracked
- **Star schema** with 4 dimensions + 1 fact table

---

## Architecture
```
Raw CSV Data (9 files, 100k+ orders)
    ↓
DuckDB Data Warehouse (11 raw tables)
    ↓
dbt Transformations (Marts Layer)
    ├── Dimension Tables (customer, product, seller, date)
    ├── Fact Table (order items with all metrics)
    └── Data Quality Tests (40 automated checks)
    ↓
Power BI Dashboard (2 pages, 10+ visualizations)
```

### Architecture Decisions

**Two-Layer Approach:**
- **Raw Layer:** Original data loaded into DuckDB (raw_customers, raw_products, etc.)
- **Marts Layer:** Analytics-ready dimensions and facts with inline data cleaning via CTEs

**Why no staging layer?**
For this project scope, I used CTEs within each mart model to handle data transformations. This simplified approach works well for the dataset size (~100k records). In a larger production environment with multiple analysts, I would add an intermediate staging layer for better modularity and reusability.

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Storage** | DuckDB | Embedded analytical database |
| **Transformation** | dbt | SQL-based data transformations |
| **Testing** | dbt tests | Automated data quality checks |
| **Languages** | SQL, Python | Data processing and scripting |
| **Visualization** | Power BI | Interactive dashboards |
| **Version Control** | Git, GitHub | Code management |

---

## Data Model

### Star Schema Design

**Fact Table:**
- `fct_order_items` - **112,101 rows** (grain: one order line item per row)
  - Measures: price, freight_value, line_total
  - Foreign keys: customer_id, product_id, seller_id, order_date
  - Degenerate dimensions: order_id, order_item_id, order_status
  - Calculated fields: delivered_on_time

**Dimension Tables:**
- `dim_customers` - **99,441 rows** with behavioral segmentation (New/One-time/Regular/VIP)
- `dim_products` - **32,951 rows** with bilingual categories (Portuguese + English)
- `dim_sellers` - **3,095 rows** with performance tiers (Inactive/New/Average/Good/Top)
- `dim_dates` - **1,461 rows** calendar table (2016-2019)

### Example: Customer Segmentation Logic
```sql
CASE 
    WHEN COALESCE(total_orders, 0) = 0 THEN 'New'
    WHEN total_orders = 1 THEN 'One-time'
    WHEN total_orders BETWEEN 2 AND 5 THEN 'Regular'
    WHEN total_orders > 5 THEN 'VIP'
END AS customer_segment
```

### Data Quality Decisions

**Issue 1: Orders Without Items (775 records)**
- **Root cause:** 78% unavailable (stock-outs), 21% canceled
- **Solution:** Excluded from fact table using INNER JOIN
- **Impact:** Ensures accurate revenue calculations

**Issue 2: Products Without Categories (610 records)**
- **Root cause:** Missing data in source
- **Solution:** Labeled as "Uncategorized" 
- **Impact:** Retains products in analysis, prevents data loss

**Issue 3: Delivered Orders Without Dates (8 records)**
- **Root cause:** Data entry errors
- **Solution:** Excluded from delivery performance metrics
- **Impact:** Maintains 92.1% on-time delivery accuracy

---

### Interactive Features

**Page 1: Executive Dashboard**
- KPI Cards: Total Revenue, Orders, Customers, Avg Order Value
- Monthly Revenue Trend (line chart with 7-day moving average)
- Revenue by Product Category (pie chart)
- Top 10 Cities by Revenue (bar chart)
- Customer Segment Distribution (donut chart)
- Date Range Slicer (interactive filter)

**Page 2: Product & Seller Analysis**
- Top Product Categories (table with conditional formatting)
- Top 10 Sellers Performance (detailed metrics)
- Delivery Performance (92.1% on-time visualization)
- Seller Performance Tier Distribution (bar chart)

### Key Insights Discovered

- **Geographic Concentration:** São Paulo accounts for $2.15M (largest market)
- **Category Leader:** Health & Beauty generates $1.4M revenue
- **Customer Opportunity:** 98k one-time buyers represent conversion potential
- **Operational Excellence:** 92.1% on-time delivery rate
- **Seller Performance:** 89 "Top Performer" tier sellers drive significant volume

---

## Getting Started

### Prerequisites
- Python 3.8+
- Git
- Power BI Desktop (optional, for viewing dashboard)

### Installation
```bash
# Clone the repository
git clone https://github.com/SakethreddyPatla/ecommerce-analytics-dbt.git
cd ecommerce-analytics-dbt

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install dbt-duckdb pandas duckdb
```

### Setup Data
```bash
# Download Olist Brazilian E-Commerce dataset
# Visit: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
# Extract CSV files to: olist_data/ folder

# Load raw data into DuckDB
python load_to_database.py
```

### Run dbt Pipeline
```bash
# Navigate to dbt project
cd ecommerce_analytics

# Run transformations (builds all dimensions and fact table)
dbt run

# Expected output: 5 models created successfully
```

### Run Data Quality Tests
```bash
# Execute all automated tests
dbt test

# Expected output: 40 tests passing 
```

### View Documentation
```bash
# Generate and serve dbt docs
dbt docs generate
dbt docs serve

# Opens browser with interactive data lineage and documentation
```

---

## Data Quality Testing

### Test Coverage: 40 Automated Tests

**Uniqueness Tests (4):**
- Primary keys unique in all dimension tables
- Date uniqueness in calendar dimension

**Not Null Tests (20):**
- All primary and foreign keys
- Critical business fields (order_status, customer_segment, etc.)
- Measure fields (price, freight_value, line_total)

**Referential Integrity Tests (4):**
- fct_order_items.customer_id → dim_customers.customer_id
- fct_order_items.product_id → dim_products.product_id
- fct_order_items.seller_id → dim_sellers.seller_id
- fct_order_items.order_date → dim_dates.date_day

**Business Logic Tests (12):**
- Order status contains only valid values
- Customer segments follow defined categories
- Seller performance tiers properly classified
- Day names validate correctly

**Run tests:**
```bash
cd ecommerce_analytics
dbt test
```

**Expected Result:**
```
Done. PASS=40 WARN=0 ERROR=0 SKIP=0 TOTAL=40
```

---

## 📈 Business Questions Answered

| # | Question | Answer | Visualization |
|---|----------|--------|---------------|
| 1 | How much revenue monthly? | Peak: $578k (July 2017) | Line chart with trend |
| 2 | Top product category? | Health & Beauty: $1.4M | Pie chart breakdown |
| 3 | Which cities generate most revenue? | São Paulo: $2.15M | Bar chart top 10 |
| 4 | Which products sell most? | Bed/Bath/Table: 11k items | Product table |
| 5 | What's average order value? | $140.37 per order | KPI card |
| 6 | Top performing sellers? | 89 "Top Performer" tier | Seller table |
| 7 | Seller-product relationships? | Drill-down capability | Interactive filtering |
| 8 | Top customer segments? | 98k one-time buyers | Segment donut chart |
| 9 | Are we delivering on time? | 92.1% on-time rate | Delivery pie chart |
| 10 | Seller delivery performance? | Performance tier distribution | Bar chart |

---

## Project Structure
```
ecommerce-analytics-dbt/
├── ecommerce_analytics/              # dbt project
│   ├── models/
│   │   ├── dim_customers.sql        # Customer dimension with segments
│   │   ├── dim_products.sql         # Product dimension with translations
│   │   ├── dim_sellers.sql          # Seller dimension with tiers
│   │   ├── dim_dates.sql            # Calendar dimension
│   │   ├── fct_order_items.sql      # Fact table with all metrics
│   │   └── schema.yml               # 40 data quality tests
│   │
│   ├── dbt_project.yml              # dbt configuration
│   └── profiles.yml                 # Database connection (not in git)
│
├── dashboard.pdf                    # Full Power BI dashboard export
│   
├── load_to_database.py              # Load CSVs to DuckDB
├── export_for_powerbi.py            # Export tables for visualization
├── data_quality_checks.py           # Initial data exploration
├── .gitignore                       # Git ignore rules
├── README.md                        # This file
└── requirements.txt                 # Python dependencies
```

---

## Technical Skills Demonstrated

### SQL Proficiency
- Complex CTEs (Common Table Expressions)
- Window functions (moving averages, aggregations)
- Multiple table JOINs (INNER, LEFT)
- Aggregate functions with GROUP BY
- CASE statements for business logic
- Date manipulation and formatting
- COALESCE for NULL handling

### dbt Best Practices
- Modular model design (one model per table)
- YAML-based testing framework
- Comprehensive documentation
- Clear naming conventions
- CTE pattern for readability

### Data Modeling
- Star schema (Kimball methodology)
- Dimension and fact table design
- Grain definition (one row = one order item)
- Foreign key relationships
- Denormalization for performance
- Slowly changing dimension concepts

### Data Engineering
- ETL pipeline development
- Data quality analysis and remediation
- Python scripting for automation
- Database design and implementation
- Performance optimization
- Version control with Git

### Analytics & BI
- Business requirements gathering
- Metric definition and calculation
- Dashboard design and UX
- Data storytelling
- Interactive visualization
- KPI tracking and reporting

---

## Key Learnings & Challenges

### Challenge 1: Understanding Unfulfilled Orders
**Problem:** Found 775 orders with no order items
**Investigation:** Analyzed order_status distribution
**Discovery:** 78% were "unavailable" (stock-outs), 21% canceled
**Solution:** Excluded from fact table to maintain accurate revenue
**Lesson:** Always investigate data anomalies before deciding how to handle them

### Challenge 2: Bilingual Product Categories
**Problem:** Categories only in Portuguese
**Solution:** Joined translation table, handled NULLs with "Uncategorized"
**Impact:** Made dashboard accessible for English-speaking stakeholders
**Lesson:** Consider end-user needs when designing data models

### Challenge 3: Performance Tier Classification
**Problem:** Need to segment sellers for analysis
**Solution:** Created tiered system based on order volume
**Impact:** Identified 89 top performers generating significant revenue
**Lesson:** Calculated fields in dimensions simplify downstream analysis

### Challenge 4: Date Dimension Design
**Problem:** Need time-based analysis capabilities
**Solution:** Generated comprehensive calendar table with attributes
**Impact:** Enabled day-of-week, weekend, and monthly trend analysis
**Lesson:** Pre-calculating date attributes saves computation in reports

---

## Future Enhancements

**Data Pipeline:**
- [ ] Add staging layer for better modularity
- [ ] Implement incremental models for large fact tables
- [ ] Create slowly changing dimensions (SCD Type 2) for historical tracking
- [ ] Add data freshness checks and monitoring

**Analytics:**
- [ ] Customer lifetime value (CLV) predictions
- [ ] Cohort retention analysis
- [ ] Churn prediction modeling
- [ ] Product recommendation engine
- [ ] Demand forecasting

**Infrastructure:**
- [ ] Deploy to cloud data warehouse (Snowflake/BigQuery)
- [ ] Set up CI/CD with GitHub Actions
- [ ] Implement data observability (Great Expectations)
- [ ] Add orchestration (Airflow/Prefect)
- [ ] Create API for data access

**Dashboard:**
- [ ] Add real-time refresh capabilities
- [ ] Create mobile-friendly views
- [ ] Add predictive analytics visualizations
- [ ] Implement row-level security
- [ ] Add more advanced drill-through capabilities

---

## About the Author

**Saketh Reddy Patla**

Analytics Engineer passionate about building data infrastructure that drives business value. This project demonstrates end-to-end analytics engineering capabilities from raw data to actionable insights.



**⭐ If you found this project helpful or interesting, please consider giving it a star!**

*This portfolio project demonstrates real-world analytics engineering skills applicable to data-driven organizations.*

---

