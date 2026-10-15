"""
text_to_sql.py - Launcher for DataTalker AI Text-to-SQL Assistant (Project 1).
Queries the Project 1 Gold Lakehouse Star Schema in DuckDB.

Run from repository root:
  python part1_tech_news_lakehouse/text_to_sql.py
Or from inside part1_tech_news_lakehouse/:
  python text_to_sql.py
"""

import os
import sys

CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
PARENT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
for p in (CURRENT_DIR, PARENT_DIR):
    if p not in sys.path:
        sys.path.insert(0, p)

try:
    from ai_text_to_sql.cli import main
except ImportError:
    from part1_tech_news_lakehouse.ai_text_to_sql.cli import main

if __name__ == "__main__":
    main()
