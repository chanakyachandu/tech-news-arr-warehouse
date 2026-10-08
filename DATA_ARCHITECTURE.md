# 🏛️ Multi-Domain Data Architecture & Dimensional Modeling Guide

## 1. Enterprise Architecture Overview (Multi-Domain Medallion & Streaming Lakehouse)
This platform implements an enterprise-grade **Multi-Domain Medallion Lakehouse** (`Bronze` $\rightarrow$ `Silver` $\rightarrow$ `Gold`) combined with a **Real-Time Streaming & CDC Engine** (Event-Time Watermarking, Dead-Letter Queue `DLQ`, and atomic `MERGE INTO` reconciliation).

```
[ MULTI-SOURCE INGESTION ]
  ├─ Domain A (Batch):  tech_news.csv (750 articles), company_metadata.json (21 seed companies)
  ├─ Domain B (Batch):  E-Commerce relational tables (orders, order_details, products, customers, categories)
  └─ Domain C (Stream): Real-time JSONL event stream (data/stream_landing/)
          │
          ▼
[ 1. BRONZE LAYER: Raw Ingestion & Schema Profiling ]
  - Zero-mutation ingestion preserving full source provenance and raw line-level audit trails.
  - Structural schema validation and automated null/type profiling.
          │
          ▼
[ 2. SILVER LAYER: Cleansing, Normalization & Feature Engineering ]
  - Domain A: Canonicalize company aliases (AWS -> Amazon Web Services), standardize 19 messy categories to 7, parse multi-currency FX (EUR, GBP, JPY, midpoint ranges) to normalized USD integers.
  - Domain B: Parse ISO order dates, compute fulfillment duration (`fulfillment_days`), calculate gross, line discounts, and net financial totals.
  - Domain C: Real-time schema validation with Dead-Letter Queue (`DLQ`) routing and Event-Time Watermarking (late-event cutoff).
          │
          ▼
[ 3. GOLD LAYER: Multi-Domain Star Schema & Analytical Models ]
  - Domain A (Tech News & ARR):
    * dim_company (Master company dimension, N = 26)
    * fct_article (750 news articles with standardized dates & categories)
    * fct_arr_observation (558 point-in-time revenue observations in USD)
    * agg_company_quarterly_arr (315 quarterly financial rollups)
    * view_company_latest_arr (26 company point-in-time latest ARR snapshots)
    * ai_articles_enriched.csv (124 filtered AI/ML records with ARR > $50M)
  - Domain B (E-Commerce Omnichannel Retail):
    * dim_customer_scd2 (Slowly Changing Dimension Type 2 with historical loyalty tier versioning, N = 175)
    * dim_product (77 SKUs with category denormalization, margin & stock health flags)
    * dim_date (732 calendar date records with Year, Quarter, Month, Weekend flags)
    * fct_orders (830 orders with fulfillment duration and net financial totals)
    * fct_order_items (2,155 line items with gross, discount rate, and net totals)
    * agg_customer_rfm (89 customer 360 RFM segmentation profiles)
```

---

## 2. Dimensional Data Models

### A. Domain A: Tech News & ARR Star Schema

```
                 +--------------------------------------+
                 |             dim_company              |
                 +--------------------------------------+
                 | PK company_id (COMP001, ...)         |
                 |    company_name                      |
                 |    industry                          |
                 |    headquarters                      |
                 |    founded_year                      |
                 |    employee_count                    |
                 |    company_size_category (Small/M/L) |
                 |    is_public (True / False)          |
                 |    stock_ticker                      |
                 |    has_company_metadata              |
                 +--------------------------------------+
                        |                       |
               1-to-Many|               1-to-Many
                        v                       v
+-------------------------------+   +------------------------------------+
|          fct_article          |   |        fct_arr_observation         |
+-------------------------------+   +------------------------------------+
| PK article_id (ART0001, ...)  |   | PK observation_id (ARR0001, ...)   |
| FK company_id                 |   | FK article_id                      |
|    title                      |   | FK company_id                      |
|    category_clean             |   |    observation_date (YYYY-MM-DD)   |
|    published_date_clean       |   |    observation_year                |
|    published_year             |   |    observation_quarter             |
|    published_month            |   |    arr_usd (Normalized Integer $)  |
|    author                     |   |    arr_usd_M (ARR in Millions)     |
|    word_count                 |   +------------------------------------+
|    summary                    |
|    url                        |
+-------------------------------+
```

---

### B. Domain B: E-Commerce & Retail Star Schema (with SCD Type 2)

```
                 +--------------------------------------+
                 |          dim_customer_scd2           |
                 +--------------------------------------+
                 | PK customer_sk (CUST_SK_0001, ...)   |
                 |    customer_id (ALFKI, ...)          |
                 |    company_name                      |
                 |    contact_name                      |
                 |    city, country                     |
                 |    loyalty_tier (Bronze/Silver/Gold) |
                 |    credit_limit_usd                  |
                 |    effective_start_date              |
                 |    effective_end_date (9999-12-31)   |
                 |    is_current_flag (True / False)    |
                 +--------------------------------------+
                        |
               1-to-Many|
                        v
+-------------------------------+   +------------------------------------+
|          fct_orders           |   |            dim_product             |
+-------------------------------+   +------------------------------------+
| PK order_id (10248, ...)      |   | PK product_sk (PROD_SK_001, ...)   |
| FK customer_sk                |   |    product_id                      |
| FK customer_id                |   |    product_name                    |
| FK order_date_sk              |   |    category_name                   |
|    order_date, required_date  |   |    category_description            |
|    shipped_date               |   |    unit_price                      |
|    fulfillment_days           |   |    units_in_stock, units_on_order  |
|    is_shipped                 |   |    stock_status (In/Low/Out Stock) |
|    freight                    |   |    is_discontinued                 |
|    ship_country               |   +------------------------------------+
|    total_gross_amount         |                       |
|    total_discount_amount      |                       | 1-to-Many
|    total_net_amount           |                       v
|    total_items_count          |   +------------------------------------+
+-------------------------------+   |          fct_order_items           |
        |                           +------------------------------------+
        | 1-to-Many                 | PK order_item_sk (ITEM_SK_00001)   |
        +-------------------------->| FK order_id                        |
                                    | FK product_sk                      |
                                    | FK product_id                      |
                                    |    unit_price, quantity            |
                                    |    discount_rate                   |
                                    |    line_gross_amount               |
                                    |    discount_amount                 |
                                    |    line_net_amount                 |
                                    +------------------------------------+
```

---

## 3. Slowly Changing Dimension Type 2 (SCD 2) Mechanics

In `dim_customer_scd2`, customer records track historical loyalty tier progressions and credit limit adjustments over time without losing historical purchase alignment:

| `customer_sk` | `customer_id` | `company_name` | `loyalty_tier` | `credit_limit_usd` | `effective_start_date` | `effective_end_date` | `is_current_flag` |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :---: |
| `CUST_SK_0007` | `BERGS` | Berglunds snabbköp | **Silver** | $10,000 | 1996-01-01 | 1997-06-30 | `False` |
| `CUST_SK_0008` | `BERGS` | Berglunds snabbköp | **Gold** | $25,000 | 1997-07-01 | 1998-03-31 | `False` |
| `CUST_SK_0009` | `BERGS` | Berglunds snabbköp | **Platinum** | $50,000 | 1998-04-01 | **9999-12-31** | **`True`** |

* **Historical Point-in-Time Queries**: Joining `fct_orders.order_date BETWEEN dim_customer_scd2.effective_start_date AND dim_customer_scd2.effective_end_date` recovers the exact customer tier at the moment the purchase took place.
* **Current State Queries**: Filtering `WHERE is_current_flag = True` yields the active customer profile.

---

## 4. Real-Time Streaming & CDC Engine Architecture

The streaming CDC module (`medallion/streaming_cdc.py`) processes high-frequency JSON micro-batches from `data/stream_landing/`:

1. **Payload Schema Validation**:
   - Validates mandatory fields (`event_id`, `event_timestamp`, `op_code`, `article_id`, `company_name`).
   - Ensures valid operation codes: `'I'` (Insert), `'U'` (Update), `'D'` (Delete).
2. **Dead-Letter Queue (DLQ) Quarantining**:
   - Any corrupt record (missing primary IDs, non-numeric ARR strings) is diverted to `data/dead_letter_queue/` with execution failure metadata and UTC timestamp.
3. **Event-Time Watermarking**:
   - Configurable watermark delay window (default = 48 hours).
   - Events arriving with `event_timestamp < (current_time - watermark_window)` are dropped or logged as late arrivals to protect state stability.
4. **Atomic `MERGE INTO` CDC Reconciliation**:
   - Reconciles live insertions, financial revisions, and retractions directly against Gold star-schema tables (`fct_arr_observation`, `fct_article`, `dim_company`, `view_company_latest_arr`).

---

## 5. Core Business & Metric Calculation Rules

1. **Multi-Currency Normalization to USD**:
   - `EUR` $\times$ 1.10 = USD
   - `GBP` $\times$ 1.27 = USD
   - `JPY` $\div$ 150 = USD
   - Revenue ranges (e.g., `"$10M - $20M"`) take the arithmetic midpoint (`$15,000,000`).
2. **E-Commerce Financial Margin Math**:
   - `line_gross_amount = unitPrice * quantity`
   - `discount_amount = line_gross_amount * discount`
   - `line_net_amount = line_gross_amount - discount_amount`
3. **Customer 360 RFM Segmentation (`agg_customer_rfm`)**:
   - **Recency ($R$)**: Days between snapshot date and customer's most recent order date.
   - **Frequency ($F$)**: Total distinct order count per customer.
   - **Monetary ($M$)**: Total net spend across all orders.
   - Quartile scoring ($1 - 4$) determines segment assignment: *Champions*, *Loyal Customers*, *Recent Customers*, *At Risk - High Spenders*, *Lost Customers*.
4. **Company Size Thresholds**:
   - **Small**: $< 10,000$ employees
   - **Medium**: $10,000$ – $30,000$ employees
   - **Large**: $> 30,000$ employees
