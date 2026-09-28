"""
sql_generator.py - Part 3: Gemini AI SQL Generator & Result Explainer
Uses Gemini (via OpenAI-compatible endpoint) to convert natural language into safe DuckDB SQL,
validates through sql_guard, executes against DuckDB, and returns structured answers.
"""

import os
import sys
import duckdb
import pandas as pd
from typing import Dict, Any, Optional, Tuple
from openai import OpenAI

# Support local imports whether running inside package or from root
try:
    from .sql_guard import clean_sql_query, validate_sql_safety, enforce_row_limit
    from .schema_provider import init_duckdb_warehouse, get_warehouse_schema_context
except ImportError:
    from sql_guard import clean_sql_query, validate_sql_safety, enforce_row_limit
    from schema_provider import init_duckdb_warehouse, get_warehouse_schema_context

GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"

def _find_gemini_key() -> str:
    """Finds Gemini API key from environment variables or nearby .env files."""
    for var in ["GEMINI_API_KEY", "GOOGLE_API_KEY", "GEMINI_KEY"]:
        val = os.environ.get(var)
        if val:
            return val
    # Search upwards through all parent directories
    from pathlib import Path
    current = Path(__file__).resolve()
    for parent in [current] + list(current.parents):
        for candidate in [parent / ".env", parent / "AI_Agents" / ".env"]:
            if candidate.is_file():
                try:
                    for line in candidate.read_text(encoding="utf-8").splitlines():
                        line = line.strip()
                        if line.startswith("GEMINI_API_KEY="):
                            key = line.split("=", 1)[1].strip().strip('"').strip("'")
                            if key:
                                return key
                except Exception:
                    pass
    return ""

DEFAULT_MODELS = [
    "gemini-3.5-flash-lite",
    "gemini-3.7-flash",
    "gemini-3.6-flash"
]


def get_gemini_client(api_key: Optional[str] = None) -> OpenAI:
    """Returns an OpenAI client configured for the Gemini API endpoint."""
    key = api_key or _find_gemini_key()
    if not key:
        raise ValueError("GEMINI_API_KEY not found. Please set GEMINI_API_KEY in your environment or a .env file.")
    return OpenAI(api_key=key, base_url=GEMINI_BASE_URL)


def generate_sql(
    question: str,
    schema_context: str,
    client: Optional[OpenAI] = None,
    model: str = DEFAULT_MODELS[0]
) -> Tuple[str, Optional[str]]:
    """
    Asks Gemini to generate a valid DuckDB SQL query answering the user's question.
    Returns (sql_query, error_message).
    """
    if client is None:
        client = get_gemini_client()

    system_prompt = (
        "You are an expert Data Engineer and DuckDB SQL specialist.\n"
        "Your task is to translate natural language business questions into precise, performant DuckDB SQL queries.\n\n"
        f"{schema_context}\n\n"
        "INSTRUCTIONS:\n"
        "1. Write ONLY the raw SQL query. Do not include conversational greetings or extra explanations.\n"
        "2. If useful, wrap in ```sql ... ``` code block.\n"
        "3. Only use tables and columns defined in the schema above.\n"
        "4. Always write READ-ONLY queries (SELECT only).\n"
        "5. Join dim_company and fct_arr_observation on company_id when analyzing revenue alongside company details."
    )

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Write a DuckDB SQL query to answer: {question}"}
            ],
            temperature=0.0
        )
        raw_sql = response.choices[0].message.content or ""
        cleaned = clean_sql_query(raw_sql)
        return cleaned, None
    except Exception as e:
        return "", f"Gemini API Error: {str(e)}"


def explain_results(
    question: str,
    sql: str,
    results_df: pd.DataFrame,
    client: Optional[OpenAI] = None,
    model: str = DEFAULT_MODELS[0]
) -> str:
    """
    Provides a concise 2-sentence executive summary of the query results.
    """
    if results_df.empty:
        return "No records matched your query criteria in the warehouse."

    if client is None:
        client = get_gemini_client()

    # Convert preview of data to string
    preview = results_df.head(10).to_string(index=False)

    prompt = (
        f"User Question: {question}\n"
        f"Executed SQL: {sql}\n"
        f"Query Results:\n{preview}\n\n"
        "Provide a concise, 1-2 sentence direct answer in plain English summarizing these findings for a business executive."
    )

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a concise executive data analyst. Answer the question directly in 1-2 clear sentences using the query results."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"(Auto-summary unavailable: {e})"


def ask_warehouse(
    question: str,
    con: duckdb.DuckDBPyConnection,
    schema_context: str,
    client: Optional[OpenAI] = None
) -> Dict[str, Any]:
    """
    Full end-to-end pipeline:
    1. Gemini writes SQL
    2. sql_guard validates safety & injects LIMIT
    3. DuckDB executes
    4. Gemini summarizes findings
    """
    output: Dict[str, Any] = {
        "question": question,
        "sql": "",
        "success": False,
        "results_df": pd.DataFrame(),
        "summary": "",
        "error": None
    }

    # Step 1: Generate SQL
    raw_sql, err = generate_sql(question, schema_context, client=client)
    if err:
        output["error"] = err
        return output

    # Step 2: Validate Safety Guard
    is_safe, safety_err = validate_sql_safety(raw_sql)
    if not is_safe:
        output["error"] = safety_err
        return output

    bounded_sql = enforce_row_limit(raw_sql, default_limit=50)
    output["sql"] = bounded_sql

    # Step 3: Execute in DuckDB
    try:
        results_df = con.execute(bounded_sql).fetchdf()
        output["results_df"] = results_df
        output["success"] = True
    except Exception as db_err:
        output["error"] = f"DuckDB Execution Error: {str(db_err)}"
        return output

    # Step 4: Explain Results
    summary = explain_results(question, bounded_sql, results_df, client=client)
    output["summary"] = summary

    return output
