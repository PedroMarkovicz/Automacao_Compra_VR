# src/agents/consolidation.py
"""
Agente de Consolidação de Dados - Consolidation Agent

Função Principal:
- Segunda etapa do pipeline de processamento VR
- Consolida os dados dos diferentes arquivos Excel em um DataFrame único
- Usa a base ATIVOS como DataFrame principal para merge com outras fontes
- Adiciona informações complementares (férias, sindicato, valores)
- Marca funcionários para exclusão (estagiários, desligados, etc.)
- Realiza joins e relacionamentos entre as diferentes fontes de dados

Operações realizadas:
- Merge com dados de férias (INICIO_FERIAS, FIM_FERIAS)
- Merge com tabela sindicato x valor (VALOR_DIA por SINDICATO)
- Identificação de estagiários para exclusão
- Consolidação de todas as informações em base única
"""
import pandas as pd
import logging
from src.graph.state import VRState
from typing import Dict

logger = logging.getLogger(__name__)


class ConsolidationAgent:
    def execute(self, state: VRState) -> VRState:
        """Consolida dados em DataFrame único usando MATRICULA como chave primária"""
        logger.info("Iniciando consolidação de dados...")

        try:
            # Validar pré-condições
            if "raw_files" not in state or not state["raw_files"]:
                raise ValueError("Dados brutos não encontrados no estado")

            raw_files = state["raw_files"]

            # Validar arquivo obrigatório
            if "ativos" not in raw_files:
                raise ValueError("Arquivo ATIVOS obrigatório não encontrado")

            # Base principal: ATIVOS - usar MATRICULA como chave primária
            base_df = raw_files["ativos"].copy()
            
            # Garantir MATRICULA como índice único
            if 'MATRICULA' in base_df.columns:
                # Remover duplicatas por MATRICULA
                base_df = base_df.drop_duplicates(subset=['MATRICULA']).reset_index(drop=True)
                logger.info(f"Base ATIVOS carregada: {len(base_df)} registros únicos por MATRICULA")
            else:
                raise ValueError("Coluna MATRICULA não encontrada em ATIVOS")

            # Consolidar dados individuais por MATRICULA
            base_df = self._consolidate_employee_data(base_df, raw_files)
            
            # Adicionar mapeamento sindicato -> valor
            base_df = self._add_union_values(base_df, raw_files)
            
            # Adicionar dias úteis por sindicato  
            base_df = self._add_workdays_by_union(base_df, raw_files)

            # Atualizar estado
            state["consolidated_df"] = base_df
            state["total_employees"] = len(base_df)
            state["processing_stage"] = "consolidation_complete"
            state["success"] = True

            logger.info(f"Consolidação concluída: {len(base_df)} funcionários totais")
            logger.info(f"Colunas consolidadas: {list(base_df.columns)}")

        except Exception as e:
            logger.error(f"Erro na consolidação de dados: {str(e)}")
            if "errors" not in state:
                state["errors"] = []
            state["errors"].append({"stage": "consolidation", "error": str(e)})
            state["success"] = False
            state["processing_stage"] = "consolidation_failed"

        return state
    
    def _consolidate_employee_data(self, base_df: pd.DataFrame, raw_files: dict) -> pd.DataFrame:
        """Consolida dados complementares por MATRICULA"""
        logger.info("Consolidando dados complementares por MATRICULA...")
        
        # Adicionar informações de férias
        if "ferias" in raw_files and not raw_files["ferias"].empty:
            ferias_df = raw_files["ferias"].copy()
            if 'MATRICULA' in ferias_df.columns:
                # Manter apenas MATRICULA e DIAS DE FÉRIAS
                if 'DIAS DE FÉRIAS' in ferias_df.columns:
                    ferias_df = ferias_df[['MATRICULA', 'DIAS DE FÉRIAS']].drop_duplicates(subset=['MATRICULA'])
                    base_df = base_df.merge(ferias_df, on="MATRICULA", how="left")
                    logger.info(f"Dados de férias consolidados: {len(ferias_df)} registros")

        # Marcar exclusões por MATRICULA
        exclusion_files = {
            'estagio': 'IS_ESTAGIARIO',
            'aprendiz': 'IS_APRENDIZ', 
            'afastamentos': 'IS_AFASTADO',
            'exterior': 'IS_EXTERIOR'
        }
        
        for file_key, flag_name in exclusion_files.items():
            if file_key in raw_files and not raw_files[file_key].empty:
                exclusion_df = raw_files[file_key]
                if 'MATRICULA' in exclusion_df.columns:
                    matriculas_exclusao = set(exclusion_df['MATRICULA'].dropna())
                    base_df[flag_name] = base_df['MATRICULA'].isin(matriculas_exclusao)
                    count = sum(base_df[flag_name])
                    logger.info(f"{flag_name}: {count} funcionários identificados")
                elif 'Cadastro' in exclusion_df.columns:  # Para arquivo EXTERIOR
                    matriculas_exclusao = set(exclusion_df['Cadastro'].dropna())
                    base_df[flag_name] = base_df['MATRICULA'].isin(matriculas_exclusao)
                    count = sum(base_df[flag_name])
                    logger.info(f"{flag_name}: {count} funcionários identificados")
        
        # Marcar desligamentos com data
        if "desligados" in raw_files and not raw_files["desligados"].empty:
            desligados_df = raw_files["desligados"].copy()
            if 'MATRICULA' in desligados_df.columns:
                # Manter MATRICULA e DATA_DEMISSAO
                cols_to_keep = ['MATRICULA']
                if 'DATA_DEMISSAO' in desligados_df.columns:
                    cols_to_keep.append('DATA_DEMISSAO')
                elif 'DATA DEMISSÃO' in desligados_df.columns:
                    cols_to_keep.append('DATA DEMISSÃO')
                    desligados_df = desligados_df.rename(columns={'DATA DEMISSÃO': 'DATA_DEMISSAO'})
                
                if len(cols_to_keep) > 1:
                    desligados_df = desligados_df[cols_to_keep].drop_duplicates(subset=['MATRICULA'])
                    base_df = base_df.merge(desligados_df, on="MATRICULA", how="left")
                    
                # Criar flag IS_DESLIGADO
                base_df['IS_DESLIGADO'] = base_df['MATRICULA'].isin(set(desligados_df['MATRICULA']))
                logger.info(f"Desligados identificados: {sum(base_df['IS_DESLIGADO'])} com datas")
        
        # Adicionar informações de admissão
        if "admissao" in raw_files and not raw_files["admissao"].empty:
            admissao_df = raw_files["admissao"].copy() 
            if 'MATRICULA' in admissao_df.columns and 'Admissão' in admissao_df.columns:
                admissao_df = admissao_df[['MATRICULA', 'Admissão']].drop_duplicates(subset=['MATRICULA'])
                admissao_df = admissao_df.rename(columns={'Admissão': 'DATA_ADMISSAO'})
                base_df = base_df.merge(admissao_df, on="MATRICULA", how="left")
                logger.info(f"Dados de admissão consolidados: {len(admissao_df)} registros")
        
        return base_df
    
    def _add_union_values(self, base_df: pd.DataFrame, raw_files: dict) -> pd.DataFrame:
        """Adiciona valores diários baseados no sindicato"""
        logger.info("Adicionando valores por sindicato...")
        
        if "base_sindicato" in raw_files and not raw_files["base_sindicato"].empty:
            sindicato_df = raw_files["base_sindicato"].copy()
            
            if "SINDICATO" in sindicato_df.columns and "VALOR_DIA" in sindicato_df.columns:
                # Mapear sindicatos completos para valores
                union_value_map = dict(zip(sindicato_df["SINDICATO"], sindicato_df["VALOR_DIA"]))
                
                # Adicionar VALOR_DIA baseado no Sindicato
                if 'Sindicato' in base_df.columns:
                    base_df['VALOR_DIA'] = base_df['Sindicato'].map(union_value_map)
                    
                    # Aplicar valor padrão onde não houver mapeamento
                    base_df['VALOR_DIA'] = base_df['VALOR_DIA'].fillna(35.0)
                    
                    logger.info(f"Valores por sindicato aplicados: {sindicato_df.shape[0]} mapeamentos")
                    logger.info(f"Valores únicos encontrados: {base_df['VALOR_DIA'].unique()}")
        
        return base_df
    
    def _add_workdays_by_union(self, base_df: pd.DataFrame, raw_files: dict) -> pd.DataFrame:
        """Adiciona dias úteis baseados no sindicato"""
        logger.info("Adicionando dias úteis por sindicato...")
        
        if "base_dias_uteis" in raw_files and not raw_files["base_dias_uteis"].empty:
            dias_df = raw_files["base_dias_uteis"].copy()
            
            if "SINDICATO" in dias_df.columns and "DIAS_UTEIS" in dias_df.columns:
                # Mapear sindicatos para dias úteis
                union_days_map = dict(zip(dias_df["SINDICATO"], dias_df["DIAS_UTEIS"]))
                
                # Adicionar DIAS_UTEIS baseado no Sindicato
                if 'Sindicato' in base_df.columns:
                    base_df['DIAS_UTEIS_SINDICATO'] = base_df['Sindicato'].map(union_days_map)
                    
                    # Aplicar valor padrão onde não houver mapeamento
                    base_df['DIAS_UTEIS_SINDICATO'] = base_df['DIAS_UTEIS_SINDICATO'].fillna(22)
                    
                    logger.info(f"Dias úteis por sindicato aplicados: {dias_df.shape[0]} mapeamentos")
                    logger.info(f"Dias úteis únicos: {sorted(base_df['DIAS_UTEIS_SINDICATO'].unique())}")
        
        return base_df
