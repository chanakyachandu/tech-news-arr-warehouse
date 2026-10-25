# 🗺️ Part 2: Slowly Changing Dimensions (SCD 0, 1, 2, 3) in Pure SQL — Hands-on Guide

**Master Artifacts:**
- **Interactive Notebook:** `scd_demonstration.ipynb` (Primary Hands-on Demonstration)
- **Production SQL Script:** `scd_queries.sql` (Pure ANSI / DuckDB SQL)
- **Automated Pipeline:** `scd_pipeline.py` (`run_scd_pipeline()`)

---

## 📊 End-to-End System Architecture

```
                       [ Raw Data: data/raw/ecommerce/customers.csv ]
                                             │
                                             ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        SETUP: DUCKDB STAGING VIEW CREATION                             │
│  • v_staging_customers: Standardizes whitespace (TRIM) and cleans strings (COALESCE)   │
└────────────────────────────────────────────────────────────────────────────────────────┘
                                             │
         ┌───────────────────┬───────────────┴───────────────┬───────────────────┐
         │                   │                               │                   │
         ▼                   ▼                               ▼                   ▼
┌─────────────────┐ ┌─────────────────┐             ┌─────────────────┐ ┌─────────────────┐
│ 1. SCD TYPE 0   │ │ 2. SCD TYPE 1   │             │ 3. SCD TYPE 2   │ │ 4. SCD TYPE 3   │
│ (FIXED ATTRIB)  │ │ (OVERWRITE)     │             │ (ROW VERSION)   │ │ (ADJACENT COLS) │
│ • ON CONFLICT   │ │ • Direct UPDATE │             │ • 2-Step DML    │ │ • In-place shift│
│   DO NOTHING    │ │ • Zero history  │             │   Close + Insert│ │ • Current and   │
│ • Initial state │ │ • Overwrites in-│             │ • Full audit    │ │   Previous      │
│   is immutable  │ │   place         │             │   trail         │ │   side-by-side  │
└─────────────────┘ └─────────────────┘             └─────────────────┘ └─────────────────┘
                                                             │
                                                             ▼
                                            ┌────────────────────────────────────┐
                                            │ 5. TEMPORAL POINT-IN-TIME QUERY    │
                                            │ • SQL BETWEEN on validity range    │
                                            │ • Reconstructs historical as-of    │
                                            │   dates without external logs      │
                                            └────────────────────────────────────┘
```

---

## 📋 Hands-on Notebook Execution Flow (`scd_demonstration.ipynb`)

### Setup: DuckDB In-Memory Engine & Staging View

#### Execution Summary
- **Target View**: `v_staging_customers`
- **Core Role**: Cleans and normalizes customer profiles into a standard staging view.
- **SQL Logic**:
  ```sql
  CREATE OR REPLACE VIEW v_staging_customers AS
  SELECT 
      TRIM(customerID) AS customer_id,
      TRIM(companyName) AS company_name,
      TRIM(contactName) AS contact_name,
      COALESCE(TRIM(city), 'Unknown') AS city,
      COALESCE(TRIM(country), 'Unknown') AS country,
      COALESCE(TRIM(region), 'N/A') AS region,
      COALESCE(TRIM(phone), 'N/A') AS phone
  FROM raw_customers;
  ```
- **Dataset Size**: Exactly 91 unique customers staged.

---

### Step 1: SCD Type 0 — Fixed / Retain Original (Insert-Only)

#### Core Concept
- **Definition**: Original attributes are written once during account creation and must never be altered (e.g. original acquisition date, initial registration country, date of birth).
- **Warehouse Rule**: When upstream systems send modifications for immutable fields, the warehouse rejects or ignores them.

#### Step-by-Step Hands-on Flow
- **1. Baseline Table Creation**:
  - Table `dim_customer_scd0` is created with `customer_id VARCHAR PRIMARY KEY`.
  - Populates initial signup details: `original_country`, `original_signup_date = '1996-01-01'`, `initial_credit_limit_usd = 5000`.
- **2. 🔴 [BEFORE UPDATE ATTEMPT] Snapshot**:
  - Customer `BERGS` (`Berglunds snabbköp`) is registered in `Sweden` on `1996-01-01` with credit limit `$5,000`.
- **3. Upstream Mutation Event**:
  - Source system attempts to update customer `BERGS` country to `Norway` and credit limit to `$99,999`.
  - Pure SQL Operation:
    ```sql
    INSERT INTO dim_customer_scd0 (customer_id, company_name, contact_name, original_country, original_signup_date, initial_credit_limit_usd)
    VALUES ('BERGS', 'Berglunds snabbköp', 'Christina Berglund', 'Norway', '1998-05-01'::DATE, 99999)
    ON CONFLICT (customer_id) DO NOTHING;
    ```
- **4. 🟢 [AFTER UPDATE ATTEMPT] Snapshot**:
  - Customer `BERGS` still shows `original_country = Sweden` and `original_signup_date = 1996-01-01`.
  - The incoming modification was ignored; immutable initial attributes are permanently protected.

---

### Step 2: SCD Type 1 — In-Place Overwrite (Zero History)

#### Core Concept
- **Definition**: Destructive update where existing row attributes are directly overwritten with incoming values.
- **Warehouse Rule**: History is intentionally not preserved. Used for correcting typos or tracking current operational state where past versions have zero business value.

#### Step-by-Step Hands-on Flow
- **1. Baseline Table Creation**:
  - Table `dim_customer_scd1` is created with initial customer location and `loyalty_tier = 'Bronze'`, `credit_limit_usd = 5000`, `last_updated_at = '1996-01-01'`.
- **2. 🔴 [BEFORE UPDATE] Snapshot**:
  - Customer `BERGS` is located in `Luleå` with `Bronze` tier and `$5,000` limit.
- **3. In-Place Overwrite DML**:
  - Customer relocates to `Stockholm` and qualifies for `Platinum` tier on `1998-04-01`.
  - Pure SQL Operation:
    ```sql
    UPDATE dim_customer_scd1
    SET city = 'Stockholm',
        loyalty_tier = 'Platinum',
        credit_limit_usd = 50000,
        last_updated_at = '1998-04-01'
    WHERE customer_id = 'BERGS';
    ```
- **4. 🟢 [AFTER UPDATE] Snapshot**:
  - Customer `BERGS` displays `Stockholm`, `Platinum`, `$50,000`, `1998-04-01`.
  - The previous values (`Luleå`, `Bronze`) are permanently overwritten; zero history remains in the table.

---

### Step 3: SCD Type 2 — Historical Row Versioning (Full Audit Trail)

#### Core Concept
- **Definition**: The enterprise standard for regulatory audits, point-in-time financial joins, and tracking the complete lifecycle of an entity.
- **Warehouse Rule**: Never overwrite existing records. Changes are tracked by closing the validity window of the old version and inserting a new version row.

#### Essential Column Schema
- `customer_sk`: Synthetic surrogate primary key (`CUST_SK_0001`, `CUST_SK_0002`, etc.) distinguishing each version of a customer.
- `customer_id`: Business / natural key linking multiple versions of the same entity together.
- `effective_start_date`: Timestamp or date when this version became valid.
- `effective_end_date`: Timestamp or date when this version was superseded. Set to open sentinel date `'9999-12-31'` for currently active rows.
- `is_current_flag`: Boolean flag (`TRUE`/`FALSE`) for fast filtering of current records.

#### Step-by-Step Hands-on Flow
- **1. Baseline Table Creation**:
  - Table `dim_customer_scd2` is populated with initial version 1 rows: `effective_start_date = '1996-01-01'`, `effective_end_date = '9999-12-31'`, `is_current_flag = TRUE`.
- **2. 🔴 [BEFORE UPDATE] Snapshot**:
  - Customer `BERGS` has 1 active record: `CUST_SK_0005`, `Silver`, `Luleå`, valid `1996-01-01` to `9999-12-31`, `is_current_flag = TRUE`.
- **3. Two-Step SQL Mutation**:
  - Step A: Expire the currently active version by updating its validity end date and active flag:
    ```sql
    UPDATE dim_customer_scd2
    SET effective_end_date = '1998-03-31',
        is_current_flag = FALSE
    WHERE customer_id = 'BERGS' AND is_current_flag = TRUE;
    ```
  - Step B: Insert the new version row as active with a new surrogate key and sentinel end date:
    ```sql
    INSERT INTO dim_customer_scd2 (
        customer_sk, customer_id, company_name, contact_name, city, country,
        loyalty_tier, credit_limit_usd, effective_start_date, effective_end_date, is_current_flag
    )
    VALUES (
        'CUST_SK_0092', 'BERGS', 'Berglunds snabbköp', 'Christina Berglund', 'Stockholm', 'Sweden',
        'Platinum', 50000, '1998-04-01', '9999-12-31', TRUE
    );
    ```
- **4. 🟢 [AFTER UPDATE] Snapshot**:
  - Customer `BERGS` now contains 2 chronological rows preserved in the dimension:
    - Row 1 (Version 1): `CUST_SK_0005 | Silver | Luleå | 1996-01-01 | 1998-03-31 | FALSE` (Closed history)
    - Row 2 (Version 2): `CUST_SK_0092 | Platinum | Stockholm | 1998-04-01 | 9999-12-31 | TRUE` (Active record)

---

### Step 4: Point-in-Time Temporal Queries (As-Of Date)

#### Core Concept
- **Definition**: Reconstructing the exact historical state of an entity on any transaction date in the past without parsing event logs or audit tables.
- **Warehouse Rule**: Join transaction facts to the SCD Type 2 dimension using SQL `BETWEEN` on the transaction timestamp.

#### Step-by-Step Hands-on Flow
- **1. Historical Query (As of 1997-06-15)**:
  - Query:
    ```sql
    SELECT customer_sk, customer_id, city, loyalty_tier, credit_limit_usd, effective_start_date, effective_end_date, is_current_flag
    FROM dim_customer_scd2
    WHERE customer_id = 'BERGS'
      AND '1997-06-15' BETWEEN effective_start_date AND effective_end_date;
    ```
  - Output: Correctly returns Version 1 (`CUST_SK_0005`, `Silver`, `Luleå`, `$10,000`, `is_current_flag = FALSE`).
- **2. Current State Query (As of 1998-05-01)**:
  - Query:
    ```sql
    SELECT customer_sk, customer_id, city, loyalty_tier, credit_limit_usd, effective_start_date, effective_end_date, is_current_flag
    FROM dim_customer_scd2
    WHERE customer_id = 'BERGS'
      AND '1998-05-01' BETWEEN effective_start_date AND effective_end_date;
    ```
  - Output: Correctly returns Version 2 (`CUST_SK_0092`, `Platinum`, `Stockholm`, `$50,000`, `is_current_flag = TRUE`).

---

### Step 5: SCD Type 3 — Current vs Previous State Columns (Adjacent History)

#### Core Concept
- **Definition**: Stores the immediate past state and current state side-by-side in adjacent columns on the same row.
- **Advantage**: Zero row explosion (exactly 91 rows). Analytical queries can perform direct before-and-after lift comparisons without complex window functions like `LAG()`.

#### Step-by-Step Hands-on Flow
- **1. Baseline Table Creation**:
  - Table `dim_customer_scd3` is created with columns: `current_loyalty_tier = 'Silver'`, `previous_loyalty_tier = NULL`, `current_credit_limit_usd = 10000`, `previous_credit_limit_usd = NULL`.
- **2. 🔴 [BEFORE UPDATE] Snapshot**:
  - Customer `BERGS` shows `current_loyalty_tier = Silver`, while `previous_loyalty_tier` is empty (`NULL`).
- **3. Adjacent Column Shift DML**:
  - In a single SQL `UPDATE`, shift existing current values into previous columns, and assign new values to current columns:
    ```sql
    UPDATE dim_customer_scd3
    SET previous_loyalty_tier = current_loyalty_tier,
        previous_credit_limit_usd = current_credit_limit_usd,
        current_loyalty_tier = 'Platinum',
        current_credit_limit_usd = 50000,
        tier_change_date = '1998-04-01'
    WHERE customer_id = 'BERGS';
    ```
- **4. 🟢 [AFTER UPDATE] Snapshot**:
  - Customer `BERGS` displays:
    - `current_loyalty_tier`: `Platinum`
    - `previous_loyalty_tier`: `Silver`
    - `current_credit_limit_usd`: `50000`
    - `previous_credit_limit_usd`: `10000`
    - `tier_change_date`: `1998-04-01`
  - Both past and present states are immediately accessible in one row.

---

## 🔍 Technical Comparison & Production Best Practices

### Architectural Trade-offs
- **SCD Type 0**:
  - Row Count Behavior: Constant (exactly $N$ rows).
  - History Retention: 0% (only immutable initial state).
  - Production Use Case: Original registration date, date of birth, initial company incorporation country.
- **SCD Type 1**:
  - Row Count Behavior: Constant (exactly $N$ rows).
  - History Retention: 0% (overwrites prior state).
  - Production Use Case: Correcting spelling errors, current operational contact phone number.
- **SCD Type 2**:
  - Row Count Behavior: Expands ($N + M$ versions).
  - History Retention: 100% (complete historical audit trail).
  - Production Use Case: Regulatory audit compliance, customer tier upgrades, point-in-time financial reporting.
- **SCD Type 3**:
  - Row Count Behavior: Constant (exactly $N$ rows).
  - History Retention: Exactly 1 prior state (stored in adjacent columns).
  - Production Use Case: Marketing campaign lift analysis (comparing performance immediately before vs after a tier change).

### Production Implementation Rules
- **Avoid Row-by-Row Loops**: Never use procedural loops (e.g. Python row iteration) in production data warehouses. Always use set-based SQL `UPDATE`, `INSERT`, or atomic `MERGE INTO` operations.
- **Always Use Surrogate Keys for SCD 2**: Natural business keys (`customer_id`) are not unique in SCD Type 2 tables because an entity can have multiple historical versions. Always generate an integer or prefixed surrogate key (`customer_sk`) as the primary key.
- **Standardize Sentinel Dates**: Use open sentinel dates like `'9999-12-31'` instead of `NULL` for `effective_end_date` on active rows. This simplifies SQL joins to a standard `BETWEEN effective_start_date AND effective_end_date` without requiring awkward `OR effective_end_date IS NULL` clauses.
