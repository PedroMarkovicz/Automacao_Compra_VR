# src/agents/validation.py
"""
Agente de Validação - Validation Agent

Função Principal:
- Terceira etapa do pipeline de processamento VR
- Aplica regras de negócio para validação e exclusão de funcionários
- Remove funcionários inelegíveis (diretores, estagiários, desligados, etc.)
- Valida dados de entrada e consistência
- Marca funcionários de férias para ajuste no cálculo
"""

import pandas as pd
import logging
from datetime import datetime
import pandas as pd
from src.graph.state import VRState
from src.core.rules import VRBusinessRules
from src.core.validators import DataValidators

logger = logging.getLogger(__name__)


class ValidationAgent:
    def __init__(self):
        self.business_rules = VRBusinessRules()
        self.validators = DataValidators()
    
    def execute(self, state: VRState) -> VRState:
        """Valida dados consolidados aplicando regras de negócio por MATRICULA"""
        logger.info("Executando ValidationAgent - aplicando regras de negócio...")
        
        try:
            # Validar pré-condições
            if "consolidated_df" not in state or state["consolidated_df"] is None:
                raise ValueError("DataFrame consolidado não encontrado no estado")
            
            # Extrair DataFrame do state de forma mais robusta
            df = self._extract_dataframe_from_state(state["consolidated_df"])
            
            if df is None or df.empty:
                raise ValueError("DataFrame consolidado está vazio ou inválido")
            
            logger.info(f"Iniciando validação de {len(df)} funcionários...")
            
            # 1. Validações de integridade dos dados
            self._validate_data_integrity(df, state)
            
            # 2. Aplicar regras de exclusão individuais por MATRICULA
            df = self._apply_individual_exclusion_rules(df, state)
            
            # 3. Aplicar regra de desligamento (antes/depois dia 15)
            df = self._apply_dismissal_rules(df, state)
            
            # 4. Identificar e processar férias
            df = self._process_vacation_rules(df, state)
            
            # 5. Calcular estatísticas de validação
            self._calculate_validation_stats(df, state)
            
            # Atualizar estado
            state["validated_df"] = df
            state["processing_stage"] = "validation_complete"
            state["success"] = True
            
            logger.info(f"Validação concluída: {len(df)} funcionários elegíveis")
            
        except Exception as e:
            logger.error(f"Erro na validação: {str(e)}")
            if "errors" not in state:
                state["errors"] = []
            state["errors"].append({"stage": "validation", "error": str(e)})
            state["success"] = False
            state["processing_stage"] = "validation_failed"
        
        return state
    
    def _extract_dataframe_from_state(self, consolidated_data) -> pd.DataFrame:
        """Extrai DataFrame do state de forma robusta"""
        if isinstance(consolidated_data, pd.DataFrame):
            return consolidated_data.copy()
        elif isinstance(consolidated_data, dict):
            # Tentar chaves comuns
            for key in ['ATIVOS', 'data', 'df']:
                if key in consolidated_data and isinstance(consolidated_data[key], pd.DataFrame):
                    return consolidated_data[key].copy()
        return None
    
    def _validate_data_integrity(self, df: pd.DataFrame, state: VRState):
        """Valida integridade dos dados"""
        logger.info("Validando integridade dos dados...")
        
        # Verificar colunas obrigatórias
        required_cols = ['MATRICULA']
        optional_cols = ['TITULO DO CARGO', 'DESC. SITUACAO', 'Sindicato']
        
        # Validar MATRICULA (obrigatória)
        if 'MATRICULA' not in df.columns:
            raise ValueError("Coluna MATRICULA é obrigatória")
        
        # Verificar colunas opcionais
        missing_cols = [col for col in optional_cols if col not in df.columns]
        if missing_cols:
            logger.warning(f"Colunas opcionais ausentes: {missing_cols}")
        
        # Validar matrículas
        if 'MATRICULA' in df.columns:
            is_valid, invalid_matriculas = self.validators.validate_matricula(df['MATRICULA'])
            if not is_valid:
                logger.warning(f"Matrículas inválidas encontradas: {len(invalid_matriculas)}")
        
        # Validar consistência geral
        is_valid, errors = self.validators.validate_data_consistency(df)
        if not is_valid:
            for error in errors:
                logger.warning(f"Inconsistência de dados: {error}")
    
    def _apply_individual_exclusion_rules(self, df: pd.DataFrame, state: VRState) -> pd.DataFrame:
        """Aplica regras de exclusão individuais por MATRICULA"""
        logger.info("Aplicando regras de exclusão individuais por MATRICULA...")
        
        initial_count = len(df)
        df['ELEGIVEL'] = True
        df['MOTIVO_EXCLUSAO'] = ''
        
        exclusion_stats = {
            'estagiarios': 0,
            'aprendizes': 0, 
            'afastados': 0,
            'exterior': 0,
            'diretores': 0,
            'situacao_invalida': 0
        }
        
        # Aplicar exclusões baseadas em flags
        for idx, row in df.iterrows():
            matricula = row['MATRICULA']
            motivos = []
            
            # Verificar flags de exclusão
            if row.get('IS_ESTAGIARIO', False):
                motivos.append('Estagiário')
                exclusion_stats['estagiarios'] += 1
            
            if row.get('IS_APRENDIZ', False):
                motivos.append('Aprendiz')
                exclusion_stats['aprendizes'] += 1
            
            if row.get('IS_AFASTADO', False):
                motivos.append('Afastado')
                exclusion_stats['afastados'] += 1
                
            if row.get('IS_EXTERIOR', False):
                motivos.append('Trabalha no exterior')
                exclusion_stats['exterior'] += 1
            
            # Verificar cargo (diretor)
            cargo = str(row.get('TITULO DO CARGO', '')).upper()
            if 'DIRETOR' in cargo or 'GERENTE GERAL' in cargo or 'PRESIDENTE' in cargo:
                motivos.append('Cargo excluído')
                exclusion_stats['diretores'] += 1
            
            # Verificar situação
            situacao = str(row.get('DESC. SITUACAO', '')).upper()
            if situacao and situacao not in ['TRABALHANDO', 'FÉRIAS']:
                # Licença maternidade e auxílio doença são exclusões
                if situacao in ['LICENÇA MATERNIDADE', 'AUXÍLIO DOENÇA', 'ATESTADO']:
                    motivos.append(f'Situação: {situacao}')
                    exclusion_stats['situacao_invalida'] += 1
            
            # Aplicar exclusão se houver motivos
            if motivos:
                df.at[idx, 'ELEGIVEL'] = False
                df.at[idx, 'MOTIVO_EXCLUSAO'] = '; '.join(motivos)
                
        # Filtrar apenas elegíveis
        df_eligible = df[df['ELEGIVEL'] == True].copy().drop(['ELEGIVEL', 'MOTIVO_EXCLUSAO'], axis=1)
        
        # Logs detalhados
        total_excluded = initial_count - len(df_eligible)
        logger.info(f"=== EXCLUSÕES APLICADAS ===")
        logger.info(f"Total inicial: {initial_count}")
        logger.info(f"Total elegíveis: {len(df_eligible)}")
        logger.info(f"Total excluídos: {total_excluded}")
        
        for reason, count in exclusion_stats.items():
            if count > 0:
                logger.info(f"  - {reason}: {count}")
        
        # Atualizar estado
        state["excluded_employees"] = total_excluded
        state["exclusion_stats"] = exclusion_stats
        
        return df_eligible
    
    def _apply_dismissal_rules(self, df: pd.DataFrame, state: VRState) -> pd.DataFrame:
        """Aplica regras de desligamento (antes/depois do dia 15)"""
        logger.info("Aplicando regras de desligamento...")
        
        if 'IS_DESLIGADO' in df.columns and 'DATA_DEMISSAO' in df.columns:
            dismissal_before_15 = 0
            dismissal_after_15 = 0
            
            for idx, row in df.iterrows():
                if row['IS_DESLIGADO'] and pd.notna(row['DATA_DEMISSAO']):
                    data_demissao = pd.to_datetime(row['DATA_DEMISSAO'])
                    
                    # Se demissão foi comunicada até o dia 15, não considera para pagamento
                    if data_demissao.day <= 15:
                        df.at[idx, 'ELEGIVEL_VR'] = False
                        df.at[idx, 'MOTIVO_EXCLUSAO_VR'] = f'Demissão até dia 15 ({data_demissao.strftime("%d/%m/%Y")})'  
                        dismissal_before_15 += 1
                    else:
                        # Demissão após dia 15 = pagamento proporcional
                        df.at[idx, 'ELEGIVEL_VR'] = True
                        df.at[idx, 'TIPO_CALCULO'] = 'PROPORCIONAL'
                        df.at[idx, 'DATA_LIMITE_CALCULO'] = data_demissao
                        dismissal_after_15 += 1
                else:
                    # Funcionários ativos
                    df.at[idx, 'ELEGIVEL_VR'] = True
                    df.at[idx, 'TIPO_CALCULO'] = 'INTEGRAL'
            
            logger.info(f"Desligamentos antes do dia 15 (excluídos): {dismissal_before_15}")
            logger.info(f"Desligamentos após dia 15 (proporcionais): {dismissal_after_15}")
        else:
            # Se não há dados de desligamento, todos são elegíveis
            df['ELEGIVEL_VR'] = True
            df['TIPO_CALCULO'] = 'INTEGRAL'
        
        # Filtrar apenas elegíveis para VR
        df = df[df['ELEGIVEL_VR'] == True].copy()
        
        return df
    
    def _process_vacation_rules(self, df: pd.DataFrame, state: VRState) -> pd.DataFrame:
        """Processa regras de férias"""
        logger.info("Processando regras de férias...")
        
        # Identificar funcionários em férias
        df['EM_FERIAS'] = False
        df['DIAS_FERIAS'] = 0
        
        if 'DIAS DE FÉRIAS' in df.columns:
            vacation_employees = 0
            for idx, row in df.iterrows():
                dias_ferias = row.get('DIAS DE FÉRIAS', 0)
                if pd.notna(dias_ferias) and dias_ferias > 0:
                    df.at[idx, 'EM_FERIAS'] = True
                    df.at[idx, 'DIAS_FERIAS'] = int(dias_ferias)
                    vacation_employees += 1
                    
                    # Para férias, ajustar tipo de cálculo
                    if df.at[idx, 'TIPO_CALCULO'] == 'INTEGRAL':
                        df.at[idx, 'TIPO_CALCULO'] = 'FERIAS'
            
            logger.info(f"Funcionários em férias identificados: {vacation_employees}")
        
        return df
    
    def _calculate_validation_stats(self, df: pd.DataFrame, state: VRState):
        """Calcula estatísticas de validação detalhadas"""
        eligible_count = len(df)
        vacation_count = df.get('EM_FERIAS', pd.Series([False] * len(df))).sum()
        
        # Contadores por tipo de cálculo
        type_counts = df.get('TIPO_CALCULO', pd.Series(['INTEGRAL'] * len(df))).value_counts().to_dict()
        
        # Atualizar contadores no estado
        state["eligible_employees"] = eligible_count
        state["employees_on_vacation"] = int(vacation_count)
        state["calculation_type_distribution"] = type_counts
        
        # Estatísticas por empresa
        if 'EMPRESA' in df.columns:
            company_stats = df['EMPRESA'].value_counts().to_dict()
            state["company_distribution"] = company_stats
            logger.info(f"Distribuição por empresa: {company_stats}")
        
        # Estatísticas por sindicato
        if 'Sindicato' in df.columns:
            union_stats = df['Sindicato'].value_counts().to_dict()
            state["union_distribution"] = union_stats
            logger.info(f"Distribuição por sindicato: {dict(list(union_stats.items())[:3])}...")
        
        # Logs de resumo
        logger.info(f"=== RESUMO DA VALIDAÇÃO ===")
        logger.info(f"Funcionários elegíveis: {eligible_count}")
        logger.info(f"Funcionários em férias: {vacation_count}")
        logger.info(f"Tipos de cálculo: {type_counts}")