# src/graph/nodes.py
"""
Funções dos Nós do Grafo - Node Functions

Define as funções que serão executadas em cada nó do grafo
Cada função recebe o estado e retorna o estado atualizado
"""

import os
from src.agents.data_ingestion import DataIngestionAgent
from src.agents.consolidation import ConsolidationAgent
from src.agents.validation import ValidationAgent
from src.agents.calculation import CalculationAgent
from src.agents.report_generation import ReportGenerationAgent
from src.graph.state import VRState


# Inicializar agentes
ingestion_agent = DataIngestionAgent(os.getenv("INPUT_PATH", "data/input"))
consolidation_agent = ConsolidationAgent()
validation_agent = ValidationAgent()
calculation_agent = CalculationAgent()
report_agent = ReportGenerationAgent(os.getenv("OUTPUT_PATH", "data/output"))


def ingest_data(state: VRState) -> VRState:
    """Nó de ingestão de dados"""
    return ingestion_agent.execute(state)


def consolidate_data(state: VRState) -> VRState:
    """Nó de consolidação de dados"""
    return consolidation_agent.execute(state)


def validate_data(state: VRState) -> VRState:
    """Nó de validação de dados"""
    return validation_agent.execute(state)


def calculate_benefits(state: VRState) -> VRState:
    """Nó de cálculo de benefícios"""
    return calculation_agent.execute(state)


def generate_report(state: VRState) -> VRState:
    """Nó de geração de relatório"""
    return report_agent.execute(state)
