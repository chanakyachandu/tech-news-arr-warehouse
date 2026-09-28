"""
cli.py - Interactive Terminal Interface for DataTalker AI Text-to-SQL Assistant.
Allows users to chat with the DuckDB warehouse dataset in real-time.
"""

import sys
import os

try:
    from .schema_provider import init_duckdb_warehouse, get_warehouse_schema_context
    from .sql_generator import ask_warehouse, get_gemini_client
except ImportError:
    from schema_provider import init_duckdb_warehouse, get_warehouse_schema_context
    from sql_generator import ask_warehouse, get_gemini_client


def main():
    print("=" * 80)
    print("🤖 DATATALKER AI - NATURAL LANGUAGE TO DUCKDB SQL ASSISTANT")
    print("=" * 80)
    print("Initializing local DuckDB analytical warehouse from CSV tables...")

    try:
        con = init_duckdb_warehouse()
        schema_context = get_warehouse_schema_context(con, sample_limit=1)
        client = get_gemini_client()
        print("✅ Warehouse tables loaded (dim_company, fct_article, fct_arr_observation, etc.)")
        print("✅ Gemini AI connection initialized.")
    except Exception as e:
        print(f"❌ Initialization Failed: {e}")
        return

    print("\nAsk any question in plain English (or type 'exit' or 'quit' to close):")
    print("Examples:")
    print("  • 'Which top 5 companies have the highest ARR?'")
    print("  • 'What are the top 3 tech categories by article count?'")
    print("  • 'Show the quarterly ARR trend for OpenAI.'")
    print("-" * 80)

    while True:
        try:
            prompt = input("\n💬 Your Question > ").strip()
            if not prompt:
                continue
            if prompt.lower() in ("exit", "quit", "q"):
                print("\nGoodbye! Happy analyzing!\n")
                break

            print("\n⏳ Thinking and analyzing warehouse tables...")
            result = ask_warehouse(prompt, con, schema_context, client=client)

            if not result["success"]:
                print(f"❌ Error: {result['error']}")
                continue

            print("\n" + "=" * 80)
            print("📝 GENERATED DUCKDB SQL:")
            print("-" * 80)
            print(result["sql"])

            print("\n📊 QUERY RESULTS:")
            print("-" * 80)
            df = result["results_df"]
            if df.empty:
                print("(0 rows returned)")
            else:
                print(df.to_string(index=False))

            print("\n💡 EXECUTIVE SUMMARY:")
            print("-" * 80)
            print(result["summary"])
            print("=" * 80)

        except KeyboardInterrupt:
            print("\nSession interrupted. Exiting.")
            break
        except Exception as e:
            print(f"\n❌ Unexpected error: {e}")


if __name__ == "__main__":
    main()
