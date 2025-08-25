# src/graph/state.py
from typing import TypedDict, Dict, List, Optional
import pandas as pd


class VRState(TypedDict):
    # Dados em cada estágio
    raw_files: Dict[str, pd.DataFrame]
    consolidated_df: Optional[pd.DataFrame]
    validated_df: Optional[pd.DataFrame]
    calculated_df: Optional[pd.DataFrame]
    final_report: Optional[pd.DataFrame]

    # Metadados
    month_year: str
    total_employees: int
    eligible_employees: int
    excluded_employees: int

    # Controle
    errors: List[Dict]
    warnings: List[Dict]
    processing_stage: str
    success: bool
