"""
medallion/data_contracts.py
============================
Enterprise Data Contracts & Automated Data Quality Scorecard Engine.

Enforces SQL DDL-equivalent semantic constraints across Medallion lakehouse tables:
  1. NOT NULL Constraints (Zero null tolerance on mandatory columns)
  2. CHECK / Range Constraints (Mathematical and numerical boundaries)
  3. UNIQUE / Primary Key Constraints (Zero duplicate keys)
  4. FOREIGN KEY / Referential Integrity Constraints (Zero orphan records)
  5. ENUM / Allowed Values Constraints (Taxonomy boundaries)

Generates an automated Data Quality Scorecard exported to `data/data_quality_report.json`.
"""

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
import pandas as pd


class DataContractValidator:
    """
    Validates Lakehouse DataFrames against formal Data Contracts
    and produces an audited Quality Scorecard.
    """

    def __init__(self, output_dir: Optional[str] = None):
        if output_dir is None:
            root = Path(__file__).resolve().parent.parent
            self.output_dir = str(root / "data")
        else:
            self.output_dir = output_dir

    # -------------------------------------------------------------------------
    # Core Constraint Checkers
    # -------------------------------------------------------------------------
    @staticmethod
    def check_not_null(df: pd.DataFrame, column: str) -> Tuple[bool, int, float, str]:
        """Verifies column has 0% null values."""
        if column not in df.columns:
            return False, len(df), 0.0, f"Column '{column}' missing from DataFrame"
        null_count = int(df[column].isnull().sum())
        total = len(df)
        pass_rate = 1.0 if total == 0 else (total - null_count) / total
        passed = null_count == 0
        details = f"{null_count} nulls found out of {total} rows" if not passed else f"0 nulls across {total} rows"
        return passed, null_count, round(pass_rate * 100, 2), details

    @staticmethod
    def check_unique(df: pd.DataFrame, column: str) -> Tuple[bool, int, float, str]:
        """Verifies column values are 100% unique (Primary Key check)."""
        if column not in df.columns:
            return False, len(df), 0.0, f"Column '{column}' missing from DataFrame"
        dup_count = int(df[column].duplicated().sum())
        total = len(df)
        pass_rate = 1.0 if total == 0 else (total - dup_count) / total
        passed = dup_count == 0
        details = f"{dup_count} duplicate keys found" if not passed else f"All {total} keys are unique"
        return passed, dup_count, round(pass_rate * 100, 2), details

    @staticmethod
    def check_range(
        df: pd.DataFrame,
        column: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
        allow_null: bool = True
    ) -> Tuple[bool, int, float, str]:
        """Verifies numerical column falls within [min_val, max_val] boundaries."""
        if column not in df.columns:
            return False, len(df), 0.0, f"Column '{column}' missing from DataFrame"
        
        series = df[column]
        if allow_null:
            series = series.dropna()
        
        total = len(series)
        if total == 0:
            return True, 0, 100.0, "0 non-null values to check"

        mask = pd.Series([True] * total, index=series.index)
        if min_val is not None:
            mask = mask & (series >= min_val)
        if max_val is not None:
            mask = mask & (series <= max_val)

        violating_count = int((~mask).sum())
        pass_rate = (total - violating_count) / total
        passed = violating_count == 0
        details = (
            f"{violating_count} values outside bounds [{min_val}, {max_val}]"
            if not passed else f"All {total} values within bounds [{min_val}, {max_val}]"
        )
        return passed, violating_count, round(pass_rate * 100, 2), details

    @staticmethod
    def check_foreign_key(
        child_df: pd.DataFrame,
        child_col: str,
        parent_df: pd.DataFrame,
        parent_col: str,
        allow_null: bool = False
    ) -> Tuple[bool, int, float, str]:
        """Verifies referential integrity: all child FK values exist in parent PK set."""
        if child_col not in child_df.columns:
            return False, len(child_df), 0.0, f"Child column '{child_col}' missing"
        if parent_col not in parent_df.columns:
            return False, len(child_df), 0.0, f"Parent column '{parent_col}' missing"

        parent_keys = set(parent_df[parent_col].dropna())
        child_keys = child_df[child_col]
        if allow_null:
            child_keys = child_keys.dropna()

        total = len(child_keys)
        if total == 0:
            return True, 0, 100.0, "0 keys to check"

        orphan_mask = ~child_keys.isin(parent_keys)
        orphan_count = int(orphan_mask.sum())
        pass_rate = (total - orphan_count) / total
        passed = orphan_count == 0
        details = (
            f"{orphan_count} orphan FK values not found in parent table"
            if not passed else f"All {total} foreign keys resolve to parent table"
        )
        return passed, orphan_count, round(pass_rate * 100, 2), details

    @staticmethod
    def check_enum(
        df: pd.DataFrame,
        column: str,
        allowed_values: Set[str],
        allow_null: bool = True
    ) -> Tuple[bool, int, float, str]:
        """Verifies string column belongs to allowed category taxonomy."""
        if column not in df.columns:
            return False, len(df), 0.0, f"Column '{column}' missing"

        series = df[column]
        if allow_null:
            series = series.dropna()

        total = len(series)
        if total == 0:
            return True, 0, 100.0, "0 values to check"

        invalid_mask = ~series.isin(allowed_values)
        invalid_count = int(invalid_mask.sum())
        pass_rate = (total - invalid_count) / total
        passed = invalid_count == 0
        details = (
            f"{invalid_count} values outside allowed enum set {allowed_values}"
            if not passed else f"All {total} values match allowed categories"
        )
        return passed, invalid_count, round(pass_rate * 100, 2), details

    # -------------------------------------------------------------------------
    # Comprehensive Lakehouse Contract Evaluation
    # -------------------------------------------------------------------------
    def validate_all(
        self,
        tech_news_gold: Dict[str, pd.DataFrame],
        ecommerce_gold: Dict[str, pd.DataFrame]
    ) -> Dict[str, Any]:
        """
        Executes formal Data Contracts across both Tech News and E-Commerce Gold tables.
        Returns a complete Data Quality Scorecard.
        """
        start_time = time.time()
        results: List[Dict[str, Any]] = []

        def _record(table: str, constraint: str, target: str, passed: bool, score: float, details: str):
            results.append({
                "table_name": table,
                "constraint_type": constraint,
                "target": target,
                "status": "PASS" if passed else "FAIL",
                "pass_rate_pct": score,
                "details": details
            })

        # =====================================================================
        # 1. DOMAIN A: TECH NEWS CONTRACTS
        # =====================================================================
        dim_company = tech_news_gold.get("dim_company", pd.DataFrame())
        fct_article = tech_news_gold.get("fct_article", pd.DataFrame())
        fct_arr = tech_news_gold.get("fct_arr_observation", pd.DataFrame())

        if not dim_company.empty:
            p, _, s, d = self.check_unique(dim_company, "company_id")
            _record("dim_company", "PRIMARY_KEY", "company_id", p, s, d)

            p, _, s, d = self.check_not_null(dim_company, "company_name")
            _record("dim_company", "NOT_NULL", "company_name", p, s, d)

        if not fct_article.empty:
            p, _, s, d = self.check_not_null(fct_article, "article_id")
            _record("fct_article", "NOT_NULL", "article_id", p, s, d)

            p, _, s, d = self.check_not_null(fct_article, "published_date_clean")
            _record("fct_article", "NOT_NULL", "published_date_clean", p, s, d)

            allowed_categories = {
                "AI_ML", "Cloud_Computing", "Data_Analytics", "FinTech", 
                "Cybersecurity", "SaaS", "Enterprise_Software", "Hardware", "Other"
            }
            p, _, s, d = self.check_enum(fct_article, "category_clean", allowed_categories, allow_null=False)
            _record("fct_article", "ENUM_TAXONOMY", "category_clean", p, s, d)

        if not fct_arr.empty and not dim_company.empty:
            p, _, s, d = self.check_foreign_key(fct_arr, "company_id", dim_company, "company_id")
            _record("fct_arr_observation", "FOREIGN_KEY", "company_id -> dim_company.company_id", p, s, d)

            p, _, s, d = self.check_range(fct_arr, "arr_usd_M", min_val=0.0, allow_null=False)
            _record("fct_arr_observation", "CHECK_RANGE", "arr_usd_M > 0", p, s, d)

            p, _, s, d = self.check_range(fct_arr, "observation_year", min_val=2000, max_val=2030, allow_null=False)
            _record("fct_arr_observation", "CHECK_RANGE", "observation_year BETWEEN 2000 AND 2030", p, s, d)

        # =====================================================================
        # 2. DOMAIN B: E-COMMERCE CONTRACTS
        # =====================================================================
        dim_cust_scd1 = ecommerce_gold.get("dim_customer_scd1", pd.DataFrame())
        dim_cust_scd2 = ecommerce_gold.get("dim_customer_scd2", pd.DataFrame())
        dim_cust_scd3 = ecommerce_gold.get("dim_customer_scd3", pd.DataFrame())
        dim_product = ecommerce_gold.get("dim_product", pd.DataFrame())
        fct_orders = ecommerce_gold.get("fct_orders", pd.DataFrame())
        fct_items = ecommerce_gold.get("fct_order_items", pd.DataFrame())
        agg_rfm = ecommerce_gold.get("agg_customer_rfm", pd.DataFrame())

        if not dim_cust_scd1.empty:
            p, _, s, d = self.check_unique(dim_cust_scd1, "customer_id")
            _record("dim_customer_scd1", "PRIMARY_KEY", "customer_id", p, s, d)

        if not dim_cust_scd2.empty:
            p, _, s, d = self.check_unique(dim_cust_scd2, "customer_sk")
            _record("dim_customer_scd2", "PRIMARY_KEY", "customer_sk", p, s, d)

            p, _, s, d = self.check_not_null(dim_cust_scd2, "effective_start_date")
            _record("dim_customer_scd2", "NOT_NULL", "effective_start_date", p, s, d)

            p, _, s, d = self.check_not_null(dim_cust_scd2, "effective_end_date")
            _record("dim_customer_scd2", "NOT_NULL", "effective_end_date", p, s, d)

        if not dim_cust_scd3.empty:
            p, _, s, d = self.check_unique(dim_cust_scd3, "customer_id")
            _record("dim_customer_scd3", "PRIMARY_KEY", "customer_id", p, s, d)

        if not dim_product.empty:
            p, _, s, d = self.check_unique(dim_product, "product_sk")
            _record("dim_product", "PRIMARY_KEY", "product_sk", p, s, d)

            p, _, s, d = self.check_range(dim_product, "unit_price", min_val=0.0)
            _record("dim_product", "CHECK_RANGE", "unit_price >= 0", p, s, d)

            allowed_stocks = {"In Stock", "Low Stock", "Out of Stock"}
            p, _, s, d = self.check_enum(dim_product, "stock_status", allowed_stocks)
            _record("dim_product", "ENUM_TAXONOMY", "stock_status", p, s, d)

        if not fct_orders.empty and not dim_cust_scd2.empty:
            p, _, s, d = self.check_foreign_key(fct_orders, "customer_sk", dim_cust_scd2, "customer_sk")
            _record("fct_orders", "FOREIGN_KEY", "customer_sk -> dim_customer_scd2.customer_sk", p, s, d)

            p, _, s, d = self.check_range(fct_orders, "total_net_amount", min_val=0.0)
            _record("fct_orders", "CHECK_RANGE", "total_net_amount >= 0", p, s, d)

        if not fct_items.empty and not fct_orders.empty and not dim_product.empty:
            p, _, s, d = self.check_foreign_key(fct_items, "order_id", fct_orders, "order_id")
            _record("fct_order_items", "FOREIGN_KEY", "order_id -> fct_orders.order_id", p, s, d)

            p, _, s, d = self.check_foreign_key(fct_items, "product_sk", dim_product, "product_sk")
            _record("fct_order_items", "FOREIGN_KEY", "product_sk -> dim_product.product_sk", p, s, d)

            p, _, s, d = self.check_range(fct_items, "discount_rate", min_val=0.0, max_val=1.0)
            _record("fct_order_items", "CHECK_RANGE", "discount_rate BETWEEN 0.0 AND 1.0", p, s, d)

            p, _, s, d = self.check_range(fct_items, "line_net_amount", min_val=0.0)
            _record("fct_order_items", "CHECK_RANGE", "line_net_amount >= 0", p, s, d)

        if not agg_rfm.empty:
            p, _, s, d = self.check_range(agg_rfm, "r_score", min_val=1, max_val=4)
            _record("agg_customer_rfm", "CHECK_RANGE", "r_score BETWEEN 1 AND 4", p, s, d)

            p, _, s, d = self.check_range(agg_rfm, "f_score", min_val=1, max_val=4)
            _record("agg_customer_rfm", "CHECK_RANGE", "f_score BETWEEN 1 AND 4", p, s, d)

            p, _, s, d = self.check_range(agg_rfm, "m_score", min_val=1, max_val=4)
            _record("agg_customer_rfm", "CHECK_RANGE", "m_score BETWEEN 1 AND 4", p, s, d)

        # =====================================================================
        # 3. Overall Scorecard Calculation
        # =====================================================================
        total_rules = len(results)
        passed_rules = sum(1 for r in results if r["status"] == "PASS")
        failed_rules = total_rules - passed_rules
        overall_health_score = round((passed_rules / total_rules * 100), 2) if total_rules > 0 else 100.0

        duration = round(time.time() - start_time, 4)
        report = {
            "evaluation_timestamp": datetime.now().isoformat(),
            "duration_seconds": duration,
            "overall_status": "PASSED" if failed_rules == 0 else "FAILED",
            "overall_health_score_pct": overall_health_score,
            "summary": {
                "total_constraints_evaluated": total_rules,
                "passed_count": passed_rules,
                "failed_count": failed_rules
            },
            "constraint_evaluations": results
        }

        # Export report to JSON
        os.makedirs(self.output_dir, exist_ok=True)
        report_path = os.path.join(self.output_dir, "data_quality_report.json")
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        return report

    def print_scorecard(self, report: Dict[str, Any]):
        """Prints a clean, formatted terminal Data Quality Scorecard."""
        print("=" * 80)
        print("[+] AUTOMATED LAKEHOUSE DATA QUALITY & CONTRACT SCORECARD")
        print("=" * 80)
        status_tag = "[PASSED]" if report["overall_status"] == "PASSED" else "[FAILED]"
        print(f"Overall Status: {status_tag} | Health Score: {report['overall_health_score_pct']}% | Duration: {report['duration_seconds']}s")
        print(f"Total Rules Checked: {report['summary']['total_constraints_evaluated']} | Passed: {report['summary']['passed_count']} | Failed: {report['summary']['failed_count']}")
        print("-" * 80)
        print(f"{'TABLE NAME':<22} | {'CONSTRAINT':<15} | {'TARGET':<30} | {'STATUS'}")
        print("-" * 80)
        for rule in report["constraint_evaluations"]:
            status_str = "[PASS]" if rule["status"] == "PASS" else "[FAIL]"
            print(f"{rule['table_name']:<22} | {rule['constraint_type']:<15} | {rule['target'][:28]:<30} | {status_str}")
        print("=" * 80)
        print(f"[+] Full JSON Report Exported to: {os.path.join(self.output_dir, 'data_quality_report.json')}")
        print("=" * 80)
