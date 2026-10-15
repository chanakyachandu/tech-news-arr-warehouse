"""
sql_guard.py - Part 2: SQL Safety Guard & Validator
Enforces strict Read-Only security on LLM-generated SQL queries before they ever reach DuckDB.
"""

import re
from typing import Tuple, Optional

# Forbidden destructive, mutation, or administrative keywords
FORBIDDEN_KEYWORDS = [
    r"\bDROP\b",
    r"\bDELETE\b",
    r"\bUPDATE\b",
    r"\bINSERT\b",
    r"\bALTER\b",
    r"\bTRUNCATE\b",
    r"\bCREATE\b",
    r"\bATTACH\b",
    r"\bDETACH\b",
    r"\bCOPY\b",
    r"\bINSTALL\b",
    r"\bLOAD\b",
    r"\bPRAGMA\b",
    r"\bEXEC\b",
    r"\bEXECUTE\b",
    r"\bREPLACE\b"
]


def clean_sql_query(raw_sql: str) -> str:
    """
    Strips markdown code blocks, backticks, and extra whitespace from LLM output.
    """
    text = raw_sql.strip()

    # Match ```sql ... ``` or ``` ... ```
    m = re.search(r"```(?:sql)?\s*(.*?)\s*```", text, re.DOTALL | re.IGNORECASE)
    if m:
        text = m.group(1).strip()

    # Remove any trailing semicolons
    text = text.rstrip(";").strip()
    return text


def validate_sql_safety(sql: str) -> Tuple[bool, Optional[str]]:
    """
    Validates that a SQL query is strictly Read-Only, single-statement, and free of dangerous commands.
    Returns (is_safe, error_message).
    """
    cleaned = clean_sql_query(sql)

    if not cleaned:
        return False, "Query is empty."

    # 1. Prevent multi-statement attacks (e.g. SELECT 1; DROP TABLE dim_company;)
    # Remove quoted strings first to avoid false positives on semicolons inside string literals
    without_strings = re.sub(r"'[^']*'", "''", cleaned)
    without_strings = re.sub(r'"[^"]*"', '""', without_strings)

    if ";" in without_strings:
        return False, "Security Violation: Multiple SQL statements detected. Only a single query is allowed."

    # 2. Must start with SELECT or WITH (for Common Table Expressions / CTEs)
    first_word_match = re.match(r"^\s*([A-Za-z]+)", without_strings)
    if not first_word_match:
        return False, "Security Violation: Unable to identify SQL statement type."

    first_word = first_word_match.group(1).upper()
    if first_word not in ("SELECT", "WITH", "DESCRIBE", "EXPLAIN"):
        return False, f"Security Violation: Statement type '{first_word}' is prohibited. Only SELECT queries are permitted."

    # 3. Check for any forbidden mutating or admin keywords
    for pattern in FORBIDDEN_KEYWORDS:
        if re.search(pattern, without_strings, re.IGNORECASE):
            keyword = pattern.replace(r"\b", "")
            return False, f"Security Violation: Forbidden keyword '{keyword}' detected. Only Read-Only queries are permitted."

    return True, None


def enforce_row_limit(sql: str, default_limit: int = 50, max_limit: int = 100) -> str:
    """
    Ensures the query has a sensible row limit so unconstrained queries do not exhaust memory.
    """
    cleaned = clean_sql_query(sql)

    # Check if query already has a LIMIT clause
    limit_match = re.search(r"\bLIMIT\s+(\d+)\s*$", cleaned, re.IGNORECASE)
    if limit_match:
        current_limit = int(limit_match.group(1))
        if current_limit > max_limit:
            # Cap at max_limit
            cleaned = re.sub(r"\bLIMIT\s+\d+\s*$", f"LIMIT {max_limit}", cleaned, flags=re.IGNORECASE)
        return cleaned
    else:
        return f"{cleaned}\nLIMIT {default_limit}"


if __name__ == "__main__":
    print("=" * 80)
    print("TESTING PART 2: SQL SAFETY GUARD & VALIDATOR")
    print("=" * 80)

    test_queries = [
        ("Valid Simple Query", "SELECT * FROM dim_company WHERE industry = 'AI/ML'"),
        ("Valid Markdown Query", "```sql\nSELECT company_name, arr_usd_M FROM view_company_latest_arr ORDER BY arr_usd_M DESC\n```"),
        ("Valid CTE Query", "WITH ranked AS (SELECT *, ROW_NUMBER() OVER(ORDER BY arr_usd DESC) as rk FROM fct_arr_observation) SELECT * FROM ranked WHERE rk <= 5"),
        ("MALICIOUS: Drop Table", "DROP TABLE dim_company;"),
        ("MALICIOUS: Embedded Semicolon Injection", "SELECT * FROM dim_company; DROP TABLE fct_article;"),
        ("MALICIOUS: Delete Statement", "DELETE FROM fct_arr_observation WHERE arr_usd < 1000000;"),
        ("MALICIOUS: Update Statement", "UPDATE dim_company SET employee_count = 0;")
    ]

    all_passed = True
    for title, q in test_queries:
        cleaned = clean_sql_query(q)
        safe, err = validate_sql_safety(cleaned)
        with_limit = enforce_row_limit(cleaned) if safe else "N/A"

        expected_safe = "MALICIOUS" not in title
        passed = (safe == expected_safe)
        if not passed:
            all_passed = False

        status = "[PASS]" if passed else "[FAIL]"
        print(f"{status} {title:<40} -> Safe: {safe} | {err or 'Approved'}")
        if safe:
            print(f"       Bounded Query: {with_limit.splitlines()[-1]}")

    print("-" * 80)
    if all_passed:
        print("[SUCCESS] Part 2 Verified: All safety rules and limit injections working 100%!\n")
    else:
        print("[!] Some safety tests failed.")
