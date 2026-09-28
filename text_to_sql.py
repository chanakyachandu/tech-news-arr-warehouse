"""
text_to_sql.py - Root Launcher for DataTalker AI Text-to-SQL Assistant.
Run with:
  "c:\\Users\\sivak\\OneDrive\\Pictures\\blank_backup\\.venv\\Scripts\\python.exe" "Fabric\\yippi\\text_to_sql.py"
"""

import os
import sys

# Ensure local package importable
CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from ai_text_to_sql.cli import main

if __name__ == "__main__":
    main()
