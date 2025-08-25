# src/config.py
"""
Configurações centralizadas do sistema
"""

from pathlib import Path


class Config:
    # Caminhos
    BASE_DIR = Path(__file__).parent.parent
    INPUT_PATH = BASE_DIR / "data" / "input"
    OUTPUT_PATH = BASE_DIR / "data" / "output"
    RULES_PATH = BASE_DIR / "data" / "rules"

    # Processamento
    PROCESSING_MONTH = "05/2025"

    # Regras de negócio
    CUTOFF_DAY = 15
    COMPANY_PERCENTAGE = 0.8
    EMPLOYEE_PERCENTAGE = 0.2

    # Cargos excluídos
    EXCLUDED_POSITIONS = ["DIRETOR", "GERENTE GERAL", "PRESIDENTE", "VICE-PRESIDENTE"]

    # Valores padrão
    DEFAULT_WORKDAYS = 22
    DEFAULT_DAILY_VALUE = 35.0

    # Logging
    LOG_LEVEL = "INFO"

    @classmethod
    def validate(cls):
        """Valida configurações"""
        if not cls.INPUT_PATH.exists():
            raise ValueError(f"Diretório de entrada não existe: {cls.INPUT_PATH}")

        cls.OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
        return True
