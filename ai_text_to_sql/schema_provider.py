"""
schema_provider.py - Part 1: Schema Context Provider & DuckDB Connection Engine
Inspects the modeled warehouse tables, loads them into DuckDB, and generates an LLM-ready schema prompt.
"""

import os
import duckdb
from typing import Dict, List, Any, Optional

WAREHOUSE_TABLES = [
    "dim_company",
    "fct_article",
    "fct_arr_observation",
    "agg_company_quarterly_arr",
    "view_company_latest_arr"
]

TABLE_DESCRIPTIONS = {
    "dim_company": "Master company dimension with industry, headquarters, headcount, founded year, public status, and size category.",
    "fct_article": "Tech news articles with publication dates, standardized categories, authors, summaries, and source URLs.",
    "fct_arr_observation": "Historical revenue/ARR observations normalized to exact USD integers and Millions (arr_usd, arr_usd_M). Excludes missing/undisclosed revenue.",
    "agg_company_quarterly_arr": "Pre-computed quarterly rollups per company (observations_count, latest_arr_usd_M, avg_arr_usd_M, max_arr_usd_M, min_arr_usd_M).",
    "view_company_latest_arr": "Latest point-in-time ARR observation snapshot per company."
}

FOREIGN_KEYS = {
    "fct_article": ["company_id -> dim_company.company_id"],
    "fct_arr_observation": [
        "article_id -> fct_article.article_id",
        "company_id -> dim_company.company_id"
    ],
    "agg_company_quarterly_arr": ["company_id -> dim_company.company_id"],
    "view_company_latest_arr": ["company_id -> dim_company.company_id"]
}


def get_default_warehouse_dir() -> str:
    """Resolves the absolute path to Fabric/yippi/data/warehouse."""
    current_dir = os.path.abspath(os.path.dirname(__file__))
    candidates = [
        os.path.abspath(os.path.join(current_dir, "..", "data", "warehouse")),
        os.path.abspath(os.path.join(current_dir, "data", "warehouse")),
        os.path.abspath(r"Fabric\yippi\data\warehouse"),
    ]
    for path in candidates:
        if os.path.exists(path) and os.path.exists(os.path.join(path, "dim_company.csv")):
            return path
    raise FileNotFoundError(f"Could not locate yippi warehouse directory in candidates: {candidates}")


def init_duckdb_warehouse(data_dir: Optional[str] = None) -> duckdb.DuckDBPyConnection:
    """
    Initializes an in-memory DuckDB database and registers all modeled warehouse CSVs as relational tables.
    """
    if data_dir is None:
        data_dir = get_default_warehouse_dir()

    con = duckdb.connect(database=":memory:")

    for table_name in WAREHOUSE_TABLES:
        csv_file = os.path.join(data_dir, f"{table_name}.csv")
        if not os.path.exists(csv_file):
            raise FileNotFoundError(f"Required warehouse table CSV missing: {csv_file}")

        # Normalize path for DuckDB SQL
        norm_path = csv_file.replace("\\", "/")
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{norm_path}')")

    return con


def get_warehouse_schema_context(con: duckdb.DuckDBPyConnection, sample_limit: int = 2) -> str:
    """
    Generates a concise, structured markdown schema prompt for the LLM.
    Includes table descriptions, column data types, foreign keys, and sample rows.
    """
    lines: List[str] = []
    lines.append("### DATABASE ENGINE: DuckDB SQL (Case-Insensitive Table and Column Names)")
    lines.append("### AVAILABLE WAREHOUSE TABLES & SCHEMAS:\n")

    for table_name in WAREHOUSE_TABLES:
        desc = TABLE_DESCRIPTIONS.get(table_name, "")
        lines.append(f"#### Table: `{table_name}`")
        lines.append(f"**Purpose**: {desc}")

        # Column schema
        schema_info = con.execute(f"DESCRIBE {table_name}").fetchall()
        cols = [f"`{col[0]}` ({col[1]})" for col in schema_info]
        lines.append(f"**Columns**: {', '.join(cols)}")

        # Foreign keys
        fks = FOREIGN_KEYS.get(table_name, [])
        if fks:
            lines.append(f"**Foreign Keys / Joins**: {'; '.join(fks)}")

        # Sample rows for grounding
        sample_rows = con.execute(f"SELECT * FROM {table_name} LIMIT {sample_limit}").fetchdf()
        # Convert to compact dict records
        sample_dicts = sample_rows.to_dict(orient="records")
        lines.append(f"**Sample Rows ({len(sample_dicts)})**:")
        for r in sample_dicts:
            # Clean string repr
            cleaned_row = {k: (str(v)[:50] + "..." if isinstance(v, str) and len(str(v)) > 50 else v) for k, v in r.items()}
            lines.append(f"  - {cleaned_row}")

        lines.append("")

    lines.append("### CRITICAL SQL QUERY CONSTRAINTS:")
    lines.append("1. Always write standard DuckDB SQL.")
    lines.append("2. Output ONLY the raw SQL query inside ```sql codeblocks or as raw text. Do not wrap in extra explanations.")
    lines.append("3. For date comparisons, use 'YYYY-MM-DD' or date functions like strftime(). Published dates are in format like 'YYYY-MM-DD' or 'DD-Mon-YY'.")
    lines.append("4. For revenue queries, use `arr_usd` (exact integer dollars) or `arr_usd_M` (integer millions).")
    lines.append("5. Missing ARR records are excluded from `fct_arr_observation`. To query articles regardless of revenue, use `fct_article`.")
    lines.append("6. Never use DROP, DELETE, INSERT, UPDATE, or ALTER.")
    lines.append("7. If calculating averages or rankings, handle NULLs properly and sort DESC/ASC as appropriate.")

    return "\n".join(lines)


if __name__ == "__main__":
    print("=" * 80)
    print("TESTING PART 1: SCHEMA CONTEXT PROVIDER & DUCKDB CONNECTION ENGINE")
    print("=" * 80)

    try:
        con = init_duckdb_warehouse()
        print("[+] DuckDB in-memory database successfully initialized!")

        for tbl in WAREHOUSE_TABLES:
            count = con.execute(f"SELECT count(*) FROM {tbl}").fetchone()[0]
            print(f"    - Table `{tbl:<26}`: {count:>5} rows loaded")

        prompt_context = get_warehouse_schema_context(con, sample_limit=1)
        print("\n[+] Generated LLM Schema Context (Character count:", len(prompt_context), "):")
        print("-" * 80)
        print(prompt_context[:1000] + "\n... [TRUNCATED FOR DISPLAY] ...")
        print("-" * 80)
        print("[SUCCESS] Part 1 Verified: All 5 warehouse tables loaded & schema prompt ready!\n")
    except Exception as e:
        print(f"[!] Error: {e}")
