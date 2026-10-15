"""
part4_data_contracts_lineage
============================
Part 4: Enterprise Data Contracts & Governance Lineage Catalog.
Enforces programmatic data quality constraints (PK, NOT NULL, Range, FK, Enum)
and generates end-to-end table/column level provenance graphs.
"""

from .data_contracts import DataContractValidator
from .lineage import DataLineageCatalog

__all__ = [
    'DataContractValidator',
    'DataLineageCatalog'
]
