"""
AI Text-to-SQL Assistant package for Yipit Analytical Data Platform.
Bridges natural language questions to safe DuckDB SQL queries over dimensional warehouse tables.
"""
from .schema_provider import init_duckdb_warehouse, get_warehouse_schema_context

__all__ = ["init_duckdb_warehouse", "get_warehouse_schema_context"]
