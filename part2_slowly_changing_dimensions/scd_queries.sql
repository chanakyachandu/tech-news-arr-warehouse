-- ==============================================================================
-- part2_slowly_changing_dimensions/scd_queries.sql
-- Pure SQL implementation of Slowly Changing Dimensions (SCD Types 0, 1, 2, 3)
-- Demonstrating Ingest, Before/After Mutations, Updates, and Point-in-Time Queries
-- Engine: DuckDB / ANSI SQL compliant
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. BASELINE SETUP: Staging Customers View
-- ------------------------------------------------------------------------------
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


-- ------------------------------------------------------------------------------
-- 2. SCD TYPE 0: FIXED / RETAIN ORIGINAL ATTRIBUTES (Insert-Only / Immutable)
-- Attributes never change once written. Any subsequent updates are ignored.
-- ------------------------------------------------------------------------------

-- Initial Table Population (with PRIMARY KEY for conflict management)
CREATE OR REPLACE TABLE dim_customer_scd0 (
    customer_id VARCHAR PRIMARY KEY,
    company_name VARCHAR,
    contact_name VARCHAR,
    original_country VARCHAR,
    original_signup_date DATE,
    initial_credit_limit_usd INT
);

INSERT INTO dim_customer_scd0
SELECT 
    customer_id,
    company_name,
    contact_name,
    country AS original_country,
    '1996-01-01'::DATE AS original_signup_date,
    5000 AS initial_credit_limit_usd
FROM v_staging_customers;

-- [BEFORE UPDATE ATTEMPT] Customer 'BERGS':
-- SELECT * FROM dim_customer_scd0 WHERE customer_id = 'BERGS';
-- Result: BERGS | Berglunds snabbköp | Sweden | 1996-01-01 | 5000

-- ATTEMPTED UPDATE / INCOMING SOURCE MUTATION:
-- Source system attempts to update country to 'Norway' and signup date to '1998-05-01'
INSERT INTO dim_customer_scd0 (customer_id, company_name, contact_name, original_country, original_signup_date, initial_credit_limit_usd)
VALUES ('BERGS', 'Berglunds snabbköp', 'Christina Berglund', 'Norway', '1998-05-01'::DATE, 99999)
ON CONFLICT (customer_id) DO NOTHING;

-- [AFTER UPDATE ATTEMPT] Customer 'BERGS':
-- SELECT * FROM dim_customer_scd0 WHERE customer_id = 'BERGS';
-- Result: BERGS | Berglunds snabbköp | Sweden | 1996-01-01 | 5000
-- Note: Attributes remain strictly unchanged; immutable signup details are preserved.


-- ------------------------------------------------------------------------------
-- 3. SCD TYPE 1: IN-PLACE OVERWRITE (Zero History, Latest Snapshot)
-- Directly overwrites existing row attributes with new incoming data.
-- ------------------------------------------------------------------------------

-- Initial Baseline Table
CREATE OR REPLACE TABLE dim_customer_scd1 AS
SELECT 
    customer_id,
    company_name,
    contact_name,
    city,
    country,
    'Bronze' AS loyalty_tier,
    5000 AS credit_limit_usd,
    '1996-01-01'::VARCHAR AS last_updated_at
FROM v_staging_customers;

-- [BEFORE UPDATE] Customer 'BERGS':
-- SELECT customer_id, city, loyalty_tier, credit_limit_usd, last_updated_at 
-- FROM dim_customer_scd1 WHERE customer_id = 'BERGS';
-- Result: BERGS | Luleå | Bronze | 5000 | 1996-01-01

-- IN-PLACE OVERWRITE:
-- Customer moves to Stockholm and upgrades to Platinum:
UPDATE dim_customer_scd1
SET city = 'Stockholm',
    loyalty_tier = 'Platinum',
    credit_limit_usd = 50000,
    last_updated_at = '1998-04-01'
WHERE customer_id = 'BERGS';

-- [AFTER UPDATE] Customer 'BERGS':
-- SELECT customer_id, city, loyalty_tier, credit_limit_usd, last_updated_at 
-- FROM dim_customer_scd1 WHERE customer_id = 'BERGS';
-- Result: BERGS | Stockholm | Platinum | 50000 | 1998-04-01
-- Note: 'Luleå' and 'Bronze' are permanently overwritten; prior history is gone.


-- ------------------------------------------------------------------------------
-- 4. SCD TYPE 2: HISTORICAL ROW VERSIONING (Full Audit Trail)
-- Each change creates a new row with surrogate key, date ranges, and active flag.
-- ------------------------------------------------------------------------------

-- Initial Baseline Version 1 Table
CREATE OR REPLACE TABLE dim_customer_scd2 AS
SELECT 
    'CUST_SK_' || LPAD((ROW_NUMBER() OVER (ORDER BY customer_id))::VARCHAR, 4, '0') AS customer_sk,
    customer_id,
    company_name,
    contact_name,
    city,
    country,
    'Silver' AS loyalty_tier,
    10000 AS credit_limit_usd,
    '1996-01-01'::VARCHAR AS effective_start_date,
    '9999-12-31'::VARCHAR AS effective_end_date,
    TRUE AS is_current_flag
FROM v_staging_customers;

-- [BEFORE UPDATE] Customer 'BERGS' (Version 1 active):
-- SELECT customer_sk, customer_id, city, loyalty_tier, effective_start_date, effective_end_date, is_current_flag 
-- FROM dim_customer_scd2 WHERE customer_id = 'BERGS';
-- Result: CUST_SK_0005 | BERGS | Luleå | Silver | 1996-01-01 | 9999-12-31 | TRUE

-- 2-STEP SCD TYPE 2 UPDATE:
-- Step A: Expire the currently active version (close validity window)
UPDATE dim_customer_scd2
SET effective_end_date = '1998-03-31',
    is_current_flag = FALSE
WHERE customer_id = 'BERGS' AND is_current_flag = TRUE;

-- Step B: Insert the new version as active (open sentinel date 9999-12-31)
INSERT INTO dim_customer_scd2 (
    customer_sk, customer_id, company_name, contact_name, city, country,
    loyalty_tier, credit_limit_usd, effective_start_date, effective_end_date, is_current_flag
)
VALUES (
    'CUST_SK_0092', 'BERGS', 'Berglunds snabbköp', 'Christina Berglund', 'Stockholm', 'Sweden',
    'Platinum', 50000, '1998-04-01', '9999-12-31', TRUE
);

-- [AFTER UPDATE] Customer 'BERGS' (Both versions preserved):
-- SELECT customer_sk, customer_id, city, loyalty_tier, effective_start_date, effective_end_date, is_current_flag 
-- FROM dim_customer_scd2 WHERE customer_id = 'BERGS' ORDER BY effective_start_date;
-- Result:
-- CUST_SK_0005 | BERGS | Luleå     | Silver   | 1996-01-01 | 1998-03-31 | FALSE
-- CUST_SK_0092 | BERGS | Stockholm | Platinum | 1998-04-01 | 9999-12-31 | TRUE


-- ------------------------------------------------------------------------------
-- 5. POINT-IN-TIME TEMPORAL QUERIES (AS-OF DATE)
-- ------------------------------------------------------------------------------

-- Query historical status as of 1997-06-15 (Returns Silver version in Luleå):
SELECT customer_sk, customer_id, city, loyalty_tier, credit_limit_usd, effective_start_date, effective_end_date, is_current_flag
FROM dim_customer_scd2
WHERE customer_id = 'BERGS'
  AND '1997-06-15' BETWEEN effective_start_date AND effective_end_date;

-- Query current status as of 1998-05-01 (Returns Platinum version in Stockholm):
SELECT customer_sk, customer_id, city, loyalty_tier, credit_limit_usd, effective_start_date, effective_end_date, is_current_flag
FROM dim_customer_scd2
WHERE customer_id = 'BERGS'
  AND '1998-05-01' BETWEEN effective_start_date AND effective_end_date;


-- ------------------------------------------------------------------------------
-- 6. SCD TYPE 3: CURRENT VS PREVIOUS STATE COLUMNS (Adjacent History)
-- Retains immediate past and present state side-by-side without row explosion.
-- ------------------------------------------------------------------------------

-- Initial Baseline Table
CREATE OR REPLACE TABLE dim_customer_scd3 AS
SELECT 
    customer_id,
    company_name,
    contact_name,
    city,
    country,
    'Silver' AS current_loyalty_tier,
    NULL::VARCHAR AS previous_loyalty_tier,
    10000 AS current_credit_limit_usd,
    NULL::INT AS previous_credit_limit_usd,
    NULL::VARCHAR AS tier_change_date
FROM v_staging_customers;

-- [BEFORE UPDATE] Customer 'BERGS':
-- SELECT customer_id, current_loyalty_tier, previous_loyalty_tier, current_credit_limit_usd, previous_credit_limit_usd, tier_change_date
-- FROM dim_customer_scd3 WHERE customer_id = 'BERGS';
-- Result: BERGS | Silver | NULL | 10000 | NULL | NULL

-- SHIFT CURRENT TO PREVIOUS & OVERWRITE CURRENT:
UPDATE dim_customer_scd3
SET previous_loyalty_tier = current_loyalty_tier,
    previous_credit_limit_usd = current_credit_limit_usd,
    current_loyalty_tier = 'Platinum',
    current_credit_limit_usd = 50000,
    tier_change_date = '1998-04-01'
WHERE customer_id = 'BERGS';

-- [AFTER UPDATE] Customer 'BERGS':
-- SELECT customer_id, current_loyalty_tier, previous_loyalty_tier, current_credit_limit_usd, previous_credit_limit_usd, tier_change_date
-- FROM dim_customer_scd3 WHERE customer_id = 'BERGS';
-- Result: BERGS | Platinum | Silver | 50000 | 10000 | 1998-04-01
-- Note: Current and previous values are stored side-by-side; no extra rows created.
