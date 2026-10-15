"""
part2_slowly_changing_dimensions/scd_pipeline.py
==================================================
Slowly Changing Dimensions (SCD Types 1, 2, and 3) Pipeline.

Focuses exclusively on dimensional history tracking strategies:
  - SCD Type 1: In-Place Overwrite (Zero history, latest snapshot only)
  - SCD Type 2: Historical Row Versioning (Surrogate keys, validity dates, active flag)
  - SCD Type 3: Current vs Previous State Columns (Side-by-side comparison)

Zero classes, pure functional architecture, zero unnecessary complexity.
"""

import os
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Tuple, Optional
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
    root = _find_project_root() if project_root is None else os.path.abspath(project_root)
    raw_dir = os.path.join(root, "data", "raw", "ecommerce")
    processed_dir = os.path.join(root, "data", "processed", "ecommerce")
    warehouse_dir = os.path.join(root, "data", "warehouse", "ecommerce")

    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    os.makedirs(warehouse_dir, exist_ok=True)

    return {
        "root": root,
        "raw_dir": raw_dir,
        "processed_dir": processed_dir,
        "warehouse_dir": warehouse_dir
    }


def load_raw_customers(raw_dir: str) -> pd.DataFrame:
    """Ingests raw customers dataset."""
    fpath = os.path.join(raw_dir, "customers.csv")
    if not os.path.exists(fpath):
        raise FileNotFoundError(f"Missing raw customers CSV: {fpath}")
    return pd.read_csv(fpath)


def load_customer_spend(raw_dir: str) -> Dict[str, float]:
    """
    Calculates total net customer spend across historical orders to derive
    realistic loyalty tiers (Bronze, Silver, Gold, Platinum).
    """
    ord_path = os.path.join(raw_dir, "orders.csv")
    det_path = os.path.join(raw_dir, "order_details.csv")

    if not os.path.exists(ord_path) or not os.path.exists(det_path):
        return {}

    df_ord = pd.read_csv(ord_path)
    df_det = pd.read_csv(det_path)

    df_det["net_line"] = (df_det["unitPrice"] * df_det["quantity"] * (1 - df_det["discount"])).round(2)
    order_totals = df_det.groupby("orderID")["net_line"].sum().reset_index()

    merged = pd.merge(df_ord[["orderID", "customerID"]], order_totals, on="orderID", how="inner")
    spend_map = merged.groupby("customerID")["net_line"].sum().round(2).to_dict()
    return spend_map


def clean_customers(df_cust: pd.DataFrame) -> pd.DataFrame:
    """Cleans customer profiles (Silver stage)."""
    clean_df = df_cust.copy()
    clean_df["companyName"] = clean_df["companyName"].str.strip()
    clean_df["contactName"] = clean_df["contactName"].str.strip()
    clean_df["city"] = clean_df["city"].fillna("Unknown").str.strip()
    clean_df["country"] = clean_df["country"].fillna("Unknown").str.strip()
    clean_df["region"] = clean_df["region"].fillna("N/A").str.strip()
    clean_df["phone"] = clean_df["phone"].fillna("N/A").str.strip()
    return clean_df


def build_scd1_dimension(df_cust: pd.DataFrame, spend_map: Dict[str, float]) -> pd.DataFrame:
    """
    SCD Type 1: In-Place Overwrite.
    Grain: Exactly 1 row per customer (91 rows).
    Preserves only the latest current tier. Overwrites historical data with no audit trail.
    """
    rows = []
    for _, r in df_cust.iterrows():
        cid = r["customerID"]
        spend = spend_map.get(cid, 0.0)

        if spend >= 15000:
            tier = "Platinum"
            credit = 50000
            updated_at = "1998-04-01"
        elif spend >= 5000:
            tier = "Gold"
            credit = 20000
            updated_at = "1998-01-01"
        else:
            tier = "Bronze"
            credit = 5000
            updated_at = "1996-01-01"

        rows.append({
            "customer_id": cid,
            "company_name": r["companyName"],
            "contact_name": r["contactName"],
            "city": r["city"],
            "country": r["country"],
            "loyalty_tier": tier,
            "credit_limit_usd": credit,
            "last_updated_at": updated_at
        })

    return pd.DataFrame(rows)


def build_scd2_dimension(df_cust: pd.DataFrame, spend_map: Dict[str, float]) -> pd.DataFrame:
    """
    SCD Type 2: Historical Row Versioning.
    Grain: 1 row per version per customer.
    Maintains full audit history using surrogate keys (customer_sk),
    effective validity date ranges (effective_start_date, effective_end_date),
    and an active current indicator (is_current_flag).
    """
    rows = []
    sk_counter = 1

    for _, r in df_cust.iterrows():
        cid = r["customerID"]
        cname = r["companyName"]
        contact = r["contactName"]
        city = r["city"]
        country = r["country"]
        spend = spend_map.get(cid, 0.0)

        if spend >= 15000:
            # Version 1: Silver (1996-01-01 to 1997-06-30) - Expired
            rows.append({
                "customer_sk": f"CUST_SK_{sk_counter:04d}",
                "customer_id": cid,
                "company_name": cname,
                "contact_name": contact,
                "city": city,
                "country": country,
                "loyalty_tier": "Silver",
                "credit_limit_usd": 10000,
                "effective_start_date": "1996-01-01",
                "effective_end_date": "1997-06-30",
                "is_current_flag": False
            })
            sk_counter += 1

            # Version 2: Gold (1997-07-01 to 1998-03-31) - Expired
            rows.append({
                "customer_sk": f"CUST_SK_{sk_counter:04d}",
                "customer_id": cid,
                "company_name": cname,
                "contact_name": contact,
                "city": city,
                "country": country,
                "loyalty_tier": "Gold",
                "credit_limit_usd": 25000,
                "effective_start_date": "1997-07-01",
                "effective_end_date": "1998-03-31",
                "is_current_flag": False
            })
            sk_counter += 1

            # Version 3: Platinum (1998-04-01 to Present) - Active
            rows.append({
                "customer_sk": f"CUST_SK_{sk_counter:04d}",
                "customer_id": cid,
                "company_name": cname,
                "contact_name": contact,
                "city": city,
                "country": country,
                "loyalty_tier": "Platinum",
                "credit_limit_usd": 50000,
                "effective_start_date": "1998-04-01",
                "effective_end_date": "9999-12-31",
                "is_current_flag": True
            })
            sk_counter += 1

        elif spend >= 5000:
            # Version 1: Bronze (1996-01-01 to 1997-12-31) - Expired
            rows.append({
                "customer_sk": f"CUST_SK_{sk_counter:04d}",
                "customer_id": cid,
                "company_name": cname,
                "contact_name": contact,
                "city": city,
                "country": country,
                "loyalty_tier": "Bronze",
                "credit_limit_usd": 5000,
                "effective_start_date": "1996-01-01",
                "effective_end_date": "1997-12-31",
                "is_current_flag": False
            })
            sk_counter += 1

            # Version 2: Gold (1998-01-01 to Present) - Active
            rows.append({
                "customer_sk": f"CUST_SK_{sk_counter:04d}",
                "customer_id": cid,
                "company_name": cname,
                "contact_name": contact,
                "city": city,
                "country": country,
                "loyalty_tier": "Gold",
                "credit_limit_usd": 20000,
                "effective_start_date": "1998-01-01",
                "effective_end_date": "9999-12-31",
                "is_current_flag": True
            })
            sk_counter += 1

        else:
            # Version 1: Bronze (1996-01-01 to Present) - Active
            rows.append({
                "customer_sk": f"CUST_SK_{sk_counter:04d}",
                "customer_id": cid,
                "company_name": cname,
                "contact_name": contact,
                "city": city,
                "country": country,
                "loyalty_tier": "Bronze",
                "credit_limit_usd": 5000,
                "effective_start_date": "1996-01-01",
                "effective_end_date": "9999-12-31",
                "is_current_flag": True
            })
            sk_counter += 1

    return pd.DataFrame(rows)


def build_scd3_dimension(df_cust: pd.DataFrame, spend_map: Dict[str, float]) -> pd.DataFrame:
    """
    SCD Type 3: Current vs Previous Column.
    Grain: Exactly 1 row per customer (91 rows).
    Tracks current and prior tier in separate columns without row explosion.
    """
    rows = []
    for _, r in df_cust.iterrows():
        cid = r["customerID"]
        spend = spend_map.get(cid, 0.0)

        if spend >= 15000:
            curr_tier = "Platinum"
            prev_tier = "Gold"
            curr_cred = 50000
            prev_cred = 25000
            change_dt = "1998-04-01"
        elif spend >= 5000:
            curr_tier = "Gold"
            prev_tier = "Bronze"
            curr_cred = 20000
            prev_cred = 5000
            change_dt = "1998-01-01"
        else:
            curr_tier = "Bronze"
            prev_tier = None
            curr_cred = 5000
            prev_cred = None
            change_dt = None

        rows.append({
            "customer_id": cid,
            "company_name": r["companyName"],
            "contact_name": r["contactName"],
            "city": r["city"],
            "country": r["country"],
            "current_loyalty_tier": curr_tier,
            "previous_loyalty_tier": prev_tier,
            "current_credit_limit_usd": curr_cred,
            "previous_credit_limit_usd": prev_cred,
            "tier_change_date": change_dt
        })

    return pd.DataFrame(rows)


def point_in_time_query(df_scd2: pd.DataFrame, customer_id: str, as_of_date: str) -> Optional[Dict[str, Any]]:
    """
    Point-in-Time Temporal Query:
    Finds the exact customer version that was active on `as_of_date`.
    Condition: effective_start_date <= as_of_date AND as_of_date <= effective_end_date.
    """
    matches = df_scd2[
        (df_scd2["customer_id"] == customer_id) &
        (df_scd2["effective_start_date"] <= as_of_date) &
        (df_scd2["effective_end_date"] >= as_of_date)
    ]
    if matches.empty:
        return None
    return matches.iloc[0].to_dict()


def run_scd_pipeline(project_root: Optional[str] = None) -> Dict[str, Any]:
    """
    Executes end-to-end SCD pipeline:
      1. Ingests raw customer dataset
      2. Cleans customer fields in Silver stage
      3. Builds SCD Type 1, 2, and 3 Gold dimensions
      4. Exports warehouse CSV deliverables
    """
    start_time = time.time()
    paths = get_ecommerce_paths(project_root)

    print("=" * 80)
    print("[+] EXECUTING SLOWLY CHANGING DIMENSIONS (SCD 1, 2, 3) PIPELINE")
    print("=" * 80)

    # 1. Ingest Raw
    df_raw_cust = load_raw_customers(paths["raw_dir"])
    spend_map = load_customer_spend(paths["raw_dir"])
    print(f"\n---> [Bronze] Ingested {len(df_raw_cust)} raw customer records")

    # 2. Clean Silver
    df_silver_cust = clean_customers(df_raw_cust)
    df_silver_cust.to_csv(os.path.join(paths["processed_dir"], "silver_customers.csv"), index=False)
    print(f"---> [Silver] Standardized customer profiles -> {paths['processed_dir']}/silver_customers.csv")

    # 3. Gold SCD Dimensions
    df_scd1 = build_scd1_dimension(df_silver_cust, spend_map)
    df_scd2 = build_scd2_dimension(df_silver_cust, spend_map)
    df_scd3 = build_scd3_dimension(df_silver_cust, spend_map)

    gold_tables = {
        "dim_customer_scd1": df_scd1,
        "dim_customer_scd2": df_scd2,
        "dim_customer_scd3": df_scd3
    }

    for name, df in gold_tables.items():
        out_path = os.path.join(paths["warehouse_dir"], f"{name}.csv")
        df.to_csv(out_path, index=False)
        print(f"---> [Gold] Materialized `{name}`: {len(df)} rows | {len(df.columns)} cols -> {out_path}")

    duration = round(time.time() - start_time, 3)
    print("\n" + "=" * 80)
    print(f"[SUCCESS] SCD Pipeline Completed in {duration}s!")
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
