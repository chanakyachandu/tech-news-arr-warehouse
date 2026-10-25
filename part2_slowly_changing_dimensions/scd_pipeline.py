"""
part2_slowly_changing_dimensions/scd_pipeline.py
==================================================
Slowly Changing Dimensions (SCD Types 0, 1, 2, and 3) Pipeline.
Powered 100% by standard DuckDB SQL engine:
  - SCD Type 0: Fixed / Retain Original (Insert-only, never mutate)
  - SCD Type 1: In-Place Overwrite (Zero history, latest snapshot)
  - SCD Type 2: Historical Row Versioning (Surrogate keys, validity dates, active flag)
  - SCD Type 3: Current vs Previous State Columns (Side-by-side comparison)

Zero manual row iteration loops. Pure, standard SQL execution.
"""

import os
import time
from pathlib import Path
from typing import Dict, Any, Optional
import duckdb
import pandas as pd


def _find_project_root() -> Path:
    """Finds the project root directory containing 'data'."""
    current = Path(__file__).resolve().parent
    for p in [current, current.parent, current.parent.parent]:
        if (p / 'data').is_dir():
            return p
    return Path.cwd()


def get_ecommerce_paths(project_root: Optional[str] = None) -> Dict[str, str]:
    """Resolves raw, processed, and warehouse directories for SCD processing."""
    root = _find_project_root() if project_root is None else Path(project_root).resolve()
    raw_dir = root / "data" / "raw" / "ecommerce"
    processed_dir = root / "data" / "processed" / "ecommerce"
    warehouse_dir = root / "data" / "warehouse" / "ecommerce"

    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)
    warehouse_dir.mkdir(parents=True, exist_ok=True)

    return {
        "root": str(root),
        "raw_dir": str(raw_dir),
        "processed_dir": str(processed_dir),
        "warehouse_dir": str(warehouse_dir)
    }


def load_raw_customers(raw_dir: str) -> pd.DataFrame:
    """Ingests raw customers dataset."""
    fpath = os.path.join(raw_dir, "customers.csv")
    if not os.path.exists(fpath):
        raise FileNotFoundError(f"Missing raw customers CSV: {fpath}")
    return pd.read_csv(fpath)


def load_customer_spend(raw_dir: str) -> Dict[str, float]:
    """Calculates customer lifetime net spend across orders using DuckDB SQL."""
    ord_path = str(Path(raw_dir) / "orders.csv").replace("\\", "/")
    det_path = str(Path(raw_dir) / "order_details.csv").replace("\\", "/")
    if not os.path.exists(ord_path) or not os.path.exists(det_path):
        return {}

    con = duckdb.connect(database=":memory:")
    sql = f"""
        WITH order_line_totals AS (
            SELECT 
                orderID,
                ROUND(SUM(unitPrice * quantity * (1.0 - discount)), 2) AS net_order_amount
            FROM read_csv_auto('{det_path}')
            GROUP BY orderID
        )
        SELECT 
            o.customerID AS customer_id,
            ROUND(SUM(l.net_order_amount), 2) AS lifetime_spend
        FROM read_csv_auto('{ord_path}') o
        JOIN order_line_totals l ON o.orderID = l.orderID
        GROUP BY o.customerID;
    """
    df = con.execute(sql).df()
    return dict(zip(df["customer_id"], df["lifetime_spend"]))


def clean_customers(df_cust: pd.DataFrame) -> pd.DataFrame:
    """Cleans and standardizes customer profiles via SQL."""
    con = duckdb.connect(database=":memory:")
    con.register("raw_cust", df_cust)
    sql = """
        SELECT 
            TRIM(customerID) AS customer_id,
            TRIM(companyName) AS company_name,
            TRIM(contactName) AS contact_name,
            COALESCE(TRIM(city), 'Unknown') AS city,
            COALESCE(TRIM(country), 'Unknown') AS country,
            COALESCE(TRIM(region), 'N/A') AS region,
            COALESCE(TRIM(phone), 'N/A') AS phone
        FROM raw_cust;
    """
    return con.execute(sql).df()


def build_scd0_dimension(df_silver_cust: pd.DataFrame) -> pd.DataFrame:
    """SCD Type 0: Fixed / Retain Original (Insert-only, never mutate) in SQL."""
    con = duckdb.connect(database=":memory:")
    con.register("silver", df_silver_cust)
    sql = """
        SELECT 
            customer_id,
            company_name,
            contact_name,
            country AS original_country,
            '1996-01-01'::DATE AS original_signup_date,
            5000 AS initial_credit_limit_usd
        FROM silver;
    """
    return con.execute(sql).df()


def build_scd1_dimension(df_silver_cust: pd.DataFrame, spend_map: Dict[str, float]) -> pd.DataFrame:
    """SCD Type 1: In-Place Overwrite (Zero history, latest snapshot) in SQL."""
    spend_df = pd.DataFrame(list(spend_map.items()), columns=["customer_id", "lifetime_spend_usd"])
    con = duckdb.connect(database=":memory:")
    con.register("silver", df_silver_cust)
    con.register("spend", spend_df)
    sql = """
        SELECT 
            c.customer_id,
            c.company_name,
            c.contact_name,
            c.city,
            c.country,
            CASE 
                WHEN COALESCE(s.lifetime_spend_usd, 0) >= 15000 THEN 'Platinum'
                WHEN COALESCE(s.lifetime_spend_usd, 0) >= 5000  THEN 'Gold'
                ELSE 'Bronze'
            END AS loyalty_tier,
            CASE 
                WHEN COALESCE(s.lifetime_spend_usd, 0) >= 15000 THEN 50000
                WHEN COALESCE(s.lifetime_spend_usd, 0) >= 5000  THEN 20000
                ELSE 5000
            END AS credit_limit_usd,
            CASE 
                WHEN COALESCE(s.lifetime_spend_usd, 0) >= 15000 THEN '1998-04-01'
                WHEN COALESCE(s.lifetime_spend_usd, 0) >= 5000  THEN '1998-01-01'
                ELSE '1996-01-01'
            END AS last_updated_at
        FROM silver c
        LEFT JOIN spend s ON c.customer_id = s.customer_id;
    """
    return con.execute(sql).df()


def build_scd2_dimension(df_silver_cust: pd.DataFrame, spend_map: Dict[str, float]) -> pd.DataFrame:
    """SCD Type 2: Historical Row Versioning (Surrogate key, date ranges, active flag) in SQL."""
    spend_df = pd.DataFrame(list(spend_map.items()), columns=["customer_id", "lifetime_spend_usd"])
    con = duckdb.connect(database=":memory:")
    con.register("silver", df_silver_cust)
    con.register("spend", spend_df)
    sql = """
        WITH base_versions AS (
            SELECT 
                c.customer_id,
                c.company_name,
                c.contact_name,
                c.city,
                c.country,
                CASE 
                    WHEN COALESCE(s.lifetime_spend_usd, 0) >= 15000 THEN 'Silver'
                    ELSE 'Bronze'
                END AS loyalty_tier,
                CASE 
                    WHEN COALESCE(s.lifetime_spend_usd, 0) >= 15000 THEN 10000
                    ELSE 5000
                END AS credit_limit_usd,
                '1996-01-01'::VARCHAR AS effective_start_date,
                CASE 
                    WHEN COALESCE(s.lifetime_spend_usd, 0) >= 15000 THEN '1997-06-30'
                    WHEN COALESCE(s.lifetime_spend_usd, 0) >= 5000  THEN '1997-12-31'
                    ELSE '9999-12-31'
                END AS effective_end_date,
                CASE 
                    WHEN COALESCE(s.lifetime_spend_usd, 0) < 5000 THEN TRUE 
                    ELSE FALSE 
                END AS is_current_flag
            FROM silver c
            LEFT JOIN spend s ON c.customer_id = s.customer_id

            UNION ALL

            SELECT 
                c.customer_id,
                c.company_name,
                c.contact_name,
                c.city,
                c.country,
                'Gold' AS loyalty_tier,
                CASE 
                    WHEN s.lifetime_spend_usd >= 15000 THEN 25000
                    ELSE 20000
                END AS credit_limit_usd,
                CASE 
                    WHEN s.lifetime_spend_usd >= 15000 THEN '1997-07-01'
                    ELSE '1998-01-01'
                END AS effective_start_date,
                CASE 
                    WHEN s.lifetime_spend_usd >= 15000 THEN '1998-03-31'
                    ELSE '9999-12-31'
                END AS effective_end_date,
                CASE 
                    WHEN s.lifetime_spend_usd >= 15000 THEN FALSE
                    ELSE TRUE
                END AS is_current_flag
            FROM silver c
            JOIN spend s ON c.customer_id = s.customer_id
            WHERE s.lifetime_spend_usd >= 5000

            UNION ALL

            SELECT 
                c.customer_id,
                c.company_name,
                c.contact_name,
                c.city,
                c.country,
                'Platinum' AS loyalty_tier,
                50000 AS credit_limit_usd,
                '1998-04-01' AS effective_start_date,
                '9999-12-31' AS effective_end_date,
                TRUE AS is_current_flag
            FROM silver c
            JOIN spend s ON c.customer_id = s.customer_id
            WHERE s.lifetime_spend_usd >= 15000
        )
        SELECT 
            'CUST_SK_' || LPAD((ROW_NUMBER() OVER (ORDER BY customer_id, effective_start_date))::VARCHAR, 4, '0') AS customer_sk,
            customer_id,
            company_name,
            contact_name,
            city,
            country,
            loyalty_tier,
            credit_limit_usd,
            effective_start_date,
            effective_end_date,
            is_current_flag
        FROM base_versions
        ORDER BY customer_id, effective_start_date;
    """
    return con.execute(sql).df()


def build_scd3_dimension(df_silver_cust: pd.DataFrame, spend_map: Dict[str, float]) -> pd.DataFrame:
    """SCD Type 3: Current vs Previous State Columns in SQL."""
    df_scd1 = build_scd1_dimension(df_silver_cust, spend_map)
    con = duckdb.connect(database=":memory:")
    con.register("scd1", df_scd1)
    sql = """
        SELECT 
            customer_id,
            company_name,
            contact_name,
            city,
            country,
            loyalty_tier AS current_loyalty_tier,
            CASE 
                WHEN loyalty_tier = 'Platinum' THEN 'Gold'
                WHEN loyalty_tier = 'Gold' THEN 'Bronze'
                ELSE NULL
            END AS previous_loyalty_tier,
            credit_limit_usd AS current_credit_limit_usd,
            CASE 
                WHEN loyalty_tier = 'Platinum' THEN 25000
                WHEN loyalty_tier = 'Gold' THEN 5000
                ELSE NULL
            END AS previous_credit_limit_usd,
            CASE 
                WHEN loyalty_tier = 'Platinum' THEN '1998-04-01'
                WHEN loyalty_tier = 'Gold' THEN '1998-01-01'
                ELSE NULL
            END AS tier_change_date
        FROM scd1;
    """
    return con.execute(sql).df()


def point_in_time_query(df_scd2: pd.DataFrame, customer_id: str, as_of_date: str) -> Optional[Dict[str, Any]]:
    """
    Point-in-Time Temporal Query:
    Finds the exact customer version active on `as_of_date` via SQL BETWEEN condition.
    """
    con = duckdb.connect(database=":memory:")
    con.register("scd2", df_scd2)
    sql = """
        SELECT *
        FROM scd2
        WHERE customer_id = ?
          AND ? BETWEEN effective_start_date AND effective_end_date
        LIMIT 1;
    """
    res = con.execute(sql, [customer_id, as_of_date]).df()
    if res.empty:
        return None
    return res.iloc[0].to_dict()


def run_scd_pipeline(project_root: Optional[str] = None) -> Dict[str, Any]:
    """
    Executes the end-to-end SCD pipeline via native DuckDB SQL.
    Transforms raw CSVs into SCD 0, 1, 2, and 3 warehouse dimensions.
    """
    start_time = time.time()
    paths = get_ecommerce_paths(project_root)
    raw_dir = Path(paths["raw_dir"])
    processed_dir = Path(paths["processed_dir"])
    warehouse_dir = Path(paths["warehouse_dir"])

    print("=" * 80)
    print("[+] EXECUTING SLOWLY CHANGING DIMENSIONS (SCD 0, 1, 2, 3) SQL PIPELINE")
    print("=" * 80)

    # 1. Ingest Raw
    df_raw_cust = load_raw_customers(str(raw_dir))
    spend_map = load_customer_spend(str(raw_dir))
    print(f"\n---> [Bronze] Ingested {len(df_raw_cust)} raw customer records")

    # 2. Clean Silver via SQL
    df_silver_cust = clean_customers(df_raw_cust)
    silver_out = processed_dir / "silver_customers.csv"
    df_silver_cust.to_csv(silver_out, index=False)
    print(f"---> [Silver] Standardized customer profiles -> {silver_out}")

    # 3. Gold SCD Dimensions via Pure SQL
    df_scd0 = build_scd0_dimension(df_silver_cust)
    df_scd1 = build_scd1_dimension(df_silver_cust, spend_map)
    df_scd2 = build_scd2_dimension(df_silver_cust, spend_map)
    df_scd3 = build_scd3_dimension(df_silver_cust, spend_map)

    gold_tables = {
        "dim_customer_scd0": df_scd0,
        "dim_customer_scd1": df_scd1,
        "dim_customer_scd2": df_scd2,
        "dim_customer_scd3": df_scd3
    }

    for name, df in gold_tables.items():
        out_csv = warehouse_dir / f"{name}.csv"
        df.to_csv(out_csv, index=False)
        print(f"---> [Gold] Materialized `{name}`: {len(df)} rows | {len(df.columns)} cols -> {out_csv}")

    duration = round(time.time() - start_time, 3)
    print("\n" + "=" * 80)
    print(f"[SUCCESS] Pure SQL SCD Pipeline Completed in {duration}s!")
    print("=" * 80)

    return {
        "duration_sec": duration,
        "bronze_data": {"customers": df_raw_cust},
        "silver_data": {"silver_customers": df_silver_cust},
        "gold_data": gold_tables,
        "bronze_tables_count": 1,
        "silver_tables_count": 1,
        "gold_tables_count": len(gold_tables)
    }


def run_ecommerce_pipeline(project_root: Optional[str] = None) -> Dict[str, Any]:
    """Compatibility runner for existing orchestrator calls."""
    return run_scd_pipeline(project_root)


if __name__ == "__main__":
    run_scd_pipeline()
