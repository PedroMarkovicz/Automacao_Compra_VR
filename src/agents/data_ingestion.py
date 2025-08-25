# src/agents/data_ingestion.py
"""
Agente de Ingestão de Dados - Data Ingestion Agent

Função Principal:
- Responsável pela primeira etapa do pipeline de processamento VR
- Carrega e valida todos os arquivos Excel de entrada (11 arquivos)
- Padroniza os dados em formato DataFrame para processamento posterior
- Valida a presença de arquivos obrigatórios (ativos, base_sindicato, base_dias_uteis, vr_mensal)
- Calcula estatísticas iniciais (total de registros, funcionários ativos)
- Atualiza o estado compartilhado (VRState) com os dados carregados
"""

import pandas as pd
from pathlib import Path
from typing import Dict
import logging
from src.utils.excel_handler import ExcelHandler
from src.graph.state import VRState
from typing import Dict

logger = logging.getLogger(__name__)


class DataIngestionAgent:
    def __init__(self, input_path: str):
        self.input_path = Path(input_path)

    def execute(self, state: VRState) -> VRState:
        """Carrega todos os arquivos Excel"""
        logger.info("Iniciando ingestão de dados...")

        try:
            # Ler todos os arquivos
            raw_files = ExcelHandler.read_all_input_files(self.input_path)

            # Validar arquivos obrigatórios
            required_files = [
                "ativos",
                "base_sindicato",
                "base_dias_uteis",
                "vr_mensal",
            ]
            missing_files = [f for f in required_files if f not in raw_files]

            if missing_files:
                raise ValueError(
                    f"Arquivos obrigatórios não encontrados: {missing_files}"
                )

            # Padronizar nomes de colunas e preservar MATRICULA
            standardized_files = self._standardize_data_columns(raw_files)
            
            # Validar presença de MATRICULA nos arquivos principais
            self._validate_matricula_presence(standardized_files)

            # Atualizar estado
            state["raw_files"] = standardized_files
            state["processing_stage"] = "ingestion_complete"
            state["success"] = True  # Definir sucesso explicitamente

            # Estatísticas
            total_records = sum(len(df) for df in standardized_files.values() if isinstance(df, pd.DataFrame) and not df.empty)
            state["total_employees"] = len(standardized_files.get("ativos", pd.DataFrame()))

            logger.info(
                f"Carregados {len(standardized_files)} arquivos, {total_records} registros totais"
            )
            logger.info(f"Total de funcionários ativos: {state['total_employees']}")

        except Exception as e:
            logger.error(f"Erro na ingestão de dados: {str(e)}")
            if "errors" not in state:
                state["errors"] = []
            state["errors"].append({"stage": "ingestion", "error": str(e)})
            state["success"] = False
            state["processing_stage"] = "ingestion_failed"

        return state
    
    def _standardize_data_columns(self, raw_files: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Padroniza nomes de colunas e formatos de dados"""
        logger.info("Padronizando nomes de colunas...")
        
        standardized = {}
        
        for file_key, df in raw_files.items():
            try:
                if df is None:
                    standardized[file_key] = df
                    continue
                
                if not isinstance(df, pd.DataFrame):
                    logger.warning(f"{file_key}: Expected DataFrame, got {type(df)}")
                    standardized[file_key] = df
                    continue
                
                if df.empty:
                    standardized[file_key] = df
                    continue
                    
                df_copy = df.copy()
                
                # Padronizar nome da coluna MATRICULA
                for col in df_copy.columns:
                    col_upper = str(col).upper().strip()
                    if 'MATRICULA' in col_upper or 'CADASTRO' in col_upper:
                        df_copy = df_copy.rename(columns={col: 'MATRICULA'})
                        break
                
                # Aplicar padronizações específicas por arquivo
                if file_key == 'base_sindicato':
                    df_copy = self._standardize_union_data(df_copy)
                elif file_key == 'base_dias_uteis':
                    df_copy = self._standardize_workdays_data(df_copy)
                elif file_key == 'ativos':
                    df_copy = self._standardize_employees_data(df_copy)
                elif file_key == 'desligados':
                    df_copy = self._standardize_dismissals_data(df_copy)
                
                standardized[file_key] = df_copy
                
            except Exception as e:
                logger.error(f"Erro ao processar {file_key}: {e}")
                standardized[file_key] = df  # Use original data if processing fails
            
        return standardized
    
    def _standardize_union_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Padroniza dados de sindicato x valor"""
        # Mapear estados para sindicatos baseado na análise dos dados
        union_mapping = {
            'Paraná': 'SITEPD PR - SIND DOS TRAB EM EMPR PRIVADAS DE PROC DE DADOS DE CURITIBA E REGIAO METROPOLITANA',
            'Rio de Janeiro': 'SINDPD RJ - SINDICATO PROFISSIONAIS DE PROC DADOS DO RIO DE JANEIRO', 
            'Rio Grande do Sul': 'SINDPPD RS - SINDICATO DOS TRAB. EM PROC. DE DADOS RIO GRANDE DO SUL',
            'São Paulo': 'SINDPD SP - SIND.TRAB.EM PROC DADOS E EMPR.EMPRESAS PROC DADOS ESTADO DE SP.'
        }
        
        if 'ESTADO' in df.columns and 'VALOR' in df.columns:
            df_mapped = pd.DataFrame()
            df_mapped['SINDICATO'] = df['ESTADO'].map(union_mapping)
            df_mapped['VALOR_DIA'] = df['VALOR']
            # Remover linhas com valores nulos
            df_mapped = df_mapped.dropna()
            return df_mapped
        
        return df
    
    def _standardize_workdays_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Padroniza dados de dias úteis"""
        if len(df.columns) >= 2:
            col_names = ['SINDICATO', 'DIAS_UTEIS']
            df_copy = df.copy()
            
            # Renomear colunas
            for i, new_name in enumerate(col_names):
                if i < len(df_copy.columns):
                    df_copy = df_copy.rename(columns={df_copy.columns[i]: new_name})
            
            # Remover header row se presente
            if df_copy.iloc[0]['SINDICATO'] == 'SINDICADO':
                df_copy = df_copy.iloc[1:].reset_index(drop=True)
            
            # Converter DIAS_UTEIS para numérico
            df_copy['DIAS_UTEIS'] = pd.to_numeric(df_copy['DIAS_UTEIS'], errors='coerce')
            
            return df_copy.dropna()
        
        return df
    
    def _standardize_employees_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Padroniza dados dos funcionários ativos"""
        # Garantir que MATRICULA seja int
        if 'MATRICULA' in df.columns:
            df['MATRICULA'] = pd.to_numeric(df['MATRICULA'], errors='coerce').astype('Int64')
        
        return df
    
    def _standardize_dismissals_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Padroniza dados de desligamentos"""
        if 'MATRICULA' in df.columns:
            df['MATRICULA'] = pd.to_numeric(df['MATRICULA'], errors='coerce').astype('Int64')
        
        # Padronizar DATA DEMISSÃO
        if 'DATA DEMISSÃO' in df.columns:
            df['DATA_DEMISSAO'] = pd.to_datetime(df['DATA DEMISSÃO'], errors='coerce')
        
        return df
    
    def _validate_matricula_presence(self, files: Dict[str, pd.DataFrame]):
        """Valida presença de MATRICULA nos arquivos principais"""
        key_files_with_matricula = ['ativos', 'ferias', 'desligados', 'admissao', 'afastamentos', 'aprendiz', 'estagio']
        
        for file_key in key_files_with_matricula:
            if file_key in files and not files[file_key].empty:
                df = files[file_key]
                if 'MATRICULA' not in df.columns:
                    logger.warning(f"MATRICULA não encontrada em {file_key}")
                else:
                    # Validar se MATRICULA tem valores válidos
                    valid_matriculas = df['MATRICULA'].notna().sum()
                    logger.info(f"{file_key}: {valid_matriculas} matrículas válidas")
