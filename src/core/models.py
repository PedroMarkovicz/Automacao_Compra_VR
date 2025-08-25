# src/core/models.py
from pydantic import BaseModel, Field
from datetime import date
from typing import Optional, Literal
from enum import Enum


class SindicatoEnum(str, Enum):
    SINDPD = "SINDPD"
    SINDICATO_A = "SINDICATO_A"
    SINDICATO_B = "SINDICATO_B"


class Employee(BaseModel):
    matricula: str = Field(..., description="Matrícula do colaborador")
    nome: str
    cpf: str
    cargo: str
    sindicato: SindicatoEnum
    data_admissao: date
    data_desligamento: Optional[date] = None
    status: Literal["ATIVO", "FERIAS", "AFASTADO", "DESLIGADO"]

    # Flags de exclusão
    is_diretor: bool = False
    is_estagiario: bool = False
    is_aprendiz: bool = False
    is_exterior: bool = False

    # Cálculos
    dias_uteis: int = 0
    valor_dia: float = 0.0
    valor_total: float = 0.0
    valor_empresa: float = 0.0
    valor_colaborador: float = 0.0


class VRConfig(BaseModel):
    mes_referencia: str
    cutoff_day: int = 15
    percentual_empresa: float = 0.8
    percentual_colaborador: float = 0.2
