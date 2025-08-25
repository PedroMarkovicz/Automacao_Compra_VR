# src/graph/workflow.py
"""
Definição do Workflow - VR Processing Workflow

Orquestra o fluxo de processamento através dos agentes
Define a sequência de execução e as transições entre estados
"""

from langgraph.graph import StateGraph, END
from src.graph.state import VRState
from src.graph.nodes import (
    ingest_data,
    consolidate_data,
    validate_data,
    calculate_benefits,
    generate_report,
)
import logging

logger = logging.getLogger(__name__)


class VRWorkflow:
    def __init__(self):
        self.workflow = StateGraph(VRState)
        self._setup_workflow()

    def _setup_workflow(self):
        """Configura o grafo do workflow"""
        # Adicionar nós
        self.workflow.add_node("ingest", ingest_data)
        self.workflow.add_node("consolidate", consolidate_data)
        self.workflow.add_node("validate", validate_data)
        self.workflow.add_node("calculate", calculate_benefits)
        self.workflow.add_node("generate", generate_report)

        # Definir fluxo sequencial
        self.workflow.set_entry_point("ingest")
        self.workflow.add_edge("ingest", "consolidate")
        self.workflow.add_edge("consolidate", "validate")
        self.workflow.add_edge("validate", "calculate")
        self.workflow.add_edge("calculate", "generate")
        self.workflow.add_edge("generate", END)

        logger.info("Workflow configurado com sucesso")

    def compile(self):
        """Compila o grafo para execução"""
        return self.workflow.compile()
