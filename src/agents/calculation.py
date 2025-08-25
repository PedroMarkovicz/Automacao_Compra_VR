# src/agents/calculation.py
"""
Agente de Cálculo - Calculation Agent

Função Principal:
- Quarta etapa do pipeline de processamento VR
- Calcula os valores de benefício VR/VA para cada funcionário elegível
- Aplica regras sindicais e convenções coletivas
- Calcula dias úteis, valores diários e repartição empresa/colaborador
- Considera funcionários em férias e outras situações especiais
"""

import pandas as pd
import logging
from datetime import datetime
import pandas as pd
from src.graph.state import VRState
from src.core.rules import VRBusinessRules

logger = logging.getLogger(__name__)


class CalculationAgent:
    def __init__(self):
        self.business_rules = VRBusinessRules()
    
    def execute(self, state: VRState) -> VRState:
        """Calcula valores de benefício VR individuais por MATRICULA"""
        logger.info("Executando CalculationAgent - calculando benefícios VR individuais...")
        
        try:
            # Validar pré-condições
            if "validated_df" not in state or state["validated_df"] is None:
                raise ValueError("DataFrame validado não encontrado no estado")
            
            # Extrair DataFrame do state
            df = self._extract_dataframe_from_state(state["validated_df"])
            
            if df is None or df.empty:
                raise ValueError("DataFrame validado está vazio ou inválido")
            
            logger.info(f"Iniciando cálculo para {len(df)} funcionários elegíveis...")
            
            # Processar cada funcionário individualmente por MATRICULA
            df = self._calculate_individual_benefits(df, state)
            
            # Validar resultados dos cálculos
            self._validate_calculation_results(df, state)
            
            # Calcular totais e estatísticas finais
            self._calculate_final_statistics(df, state)
            
            # Atualizar estado
            state["calculated_df"] = df
            state["monthly_workdays"] = workdays
            state["processing_stage"] = "calculation_complete"
            state["success"] = True
            
            logger.info(f"Cálculo concluído para {len(df)} funcionários")
            
        except Exception as e:
            logger.error(f"Erro no cálculo: {str(e)}")
            if "errors" not in state:
                state["errors"] = []
            state["errors"].append({"stage": "calculation", "error": str(e)})
            state["success"] = False
            state["processing_stage"] = "calculation_failed"
        
        return state
    
    def _extract_dataframe_from_state(self, validated_data) -> pd.DataFrame:
        """Extrai DataFrame do state de forma robusta"""
        if isinstance(validated_data, pd.DataFrame):
            return validated_data.copy()
        elif isinstance(validated_data, dict):
            # Tentar chaves comuns
            for key in ['data', 'df', 'validated']:
                if key in validated_data and isinstance(validated_data[key], pd.DataFrame):
                    return validated_data[key].copy()
        return None
    
    def _calculate_individual_benefits(self, df: pd.DataFrame, state: VRState) -> pd.DataFrame:
        """Calcula benefícios individuais por MATRICULA"""
        logger.info("Calculando benefícios individuais por MATRICULA...")
        
        # Inicializar colunas de cálculo
        df['DIAS_CALCULO'] = 0
        df['VALOR_TOTAL_VR'] = 0.0
        df['CUSTO_EMPRESA'] = 0.0  # 80%
        df['DESCONTO_PROFISSIONAL'] = 0.0  # 20%
        df['OBSERVACOES'] = ''
        
        # Processar cada funcionário individualmente
        for idx, employee in df.iterrows():
            matricula = employee['MATRICULA']
            
            try:
                # Calcular dias para este funcionário específico
                dias_calculo = self._calculate_individual_workdays(employee, state)
                
                # Obter valor diário baseado no sindicato
                valor_diario = self._get_daily_value_for_employee(employee)
                
                # Calcular valores totais
                valor_total = dias_calculo * valor_diario
                custo_empresa = valor_total * 0.8
                desconto_profissional = valor_total * 0.2
                
                # Gerar observações
                observacoes = self._generate_employee_observations(employee, dias_calculo, valor_diario)
                
                # Atualizar DataFrame
                df.at[idx, 'DIAS_CALCULO'] = dias_calculo
                df.at[idx, 'VALOR_TOTAL_VR'] = round(valor_total, 2)
                df.at[idx, 'CUSTO_EMPRESA'] = round(custo_empresa, 2)
                df.at[idx, 'DESCONTO_PROFISSIONAL'] = round(desconto_profissional, 2)
                df.at[idx, 'OBSERVACOES'] = observacoes
                
                if idx % 100 == 0:  # Log a cada 100 registros
                    logger.info(f"Processados {idx + 1} funcionários...")
                    
            except Exception as e:
                logger.error(f"Erro ao calcular benefício para MATRICULA {matricula}: {e}")
                df.at[idx, 'OBSERVACOES'] = f'ERRO NO CÁLCULO: {str(e)}'
        
        logger.info(f"Cálculos individuais concluídos para {len(df)} funcionários")
        return df
    
    def _calculate_individual_workdays(self, employee: pd.Series, state: VRState) -> int:
        """Calcula dias úteis para um funcionário específico"""
        # Obter dias úteis base do sindicato
        dias_base = employee.get('DIAS_UTEIS_SINDICATO', 22)
        
        if pd.isna(dias_base):
            dias_base = 22
        else:
            dias_base = int(dias_base)
        
        tipo_calculo = employee.get('TIPO_CALCULO', 'INTEGRAL')
        
        if tipo_calculo == 'INTEGRAL':
            return dias_base
        
        elif tipo_calculo == 'FERIAS':
            # Calcular dias considerando férias
            dias_ferias = employee.get('DIAS_FERIAS', 0)
            if pd.isna(dias_ferias):
                dias_ferias = 0
            else:
                dias_ferias = int(dias_ferias)
            
            # Regra: reduzir dias úteis baseado nos dias de férias
            dias_ajustados = max(0, dias_base - dias_ferias)
            return dias_ajustados
            
        elif tipo_calculo == 'PROPORCIONAL':
            # Calcular proporcional baseado na data limite
            data_limite = employee.get('DATA_LIMITE_CALCULO')
            if pd.isna(data_limite):
                return dias_base
            
            # Calcular dias até a data limite (simplificado)
            data_limite = pd.to_datetime(data_limite)
            dia_limite = data_limite.day
            
            # Proporção baseada no dia do mês (aproximação)
            if dia_limite <= 15:
                return max(1, int(dias_base * 0.5))  # Primeira quinzena
            else:
                return dias_base  # Mês completo
        
        return dias_base
    
    def _get_daily_value_for_employee(self, employee: pd.Series) -> float:
        """Obtém valor diário VR para um funcionário específico"""
        # Primeiro, tentar usar valor já mapeado
        valor_dia = employee.get('VALOR_DIA', None)
        
        if pd.notna(valor_dia) and valor_dia > 0:
            return float(valor_dia)
        
        # Fallback: usar valor padrão baseado no sindicato
        sindicato = str(employee.get('Sindicato', ''))
        
        # Mapeamento baseado na análise dos dados
        sindicato_valores = {
            'SINDPD SP': 37.5,  # São Paulo
            'SINDPPD RS': 35.0,  # Rio Grande do Sul
            'SITEPD PR': 35.0,   # Paraná
            'SINDPD RJ': 35.0    # Rio de Janeiro
        }
        
        # Buscar por partes do nome do sindicato
        for key, value in sindicato_valores.items():
            if key in sindicato:
                return value
        
        # Valor padrão
        return 35.0
    
    def _generate_employee_observations(self, employee: pd.Series, dias: int, valor_diario: float) -> str:
        """Gera observações para o funcionário"""
        observacoes = []
        
        tipo_calculo = employee.get('TIPO_CALCULO', 'INTEGRAL')
        
        if tipo_calculo == 'FERIAS':
            dias_ferias = employee.get('DIAS_FERIAS', 0)
            observacoes.append(f'AJUSTE FÉRIAS: {int(dias_ferias)} dias descontados')
        
        elif tipo_calculo == 'PROPORCIONAL':
            data_limite = employee.get('DATA_LIMITE_CALCULO')
            if pd.notna(data_limite):
                data_limite = pd.to_datetime(data_limite)
                observacoes.append(f'DESLIGAMENTO PROPORCIONAL até {data_limite.strftime("%d/%m/%Y")}')
        
        # Adicionar informações do cálculo
        observacoes.append(f'{dias} dias × R$ {valor_diario:.2f}')
        
        return '; '.join(observacoes) if observacoes else 'CÁLCULO NORMAL'
    
    def _validate_calculation_results(self, df: pd.DataFrame, state: VRState):
        """Valida os resultados dos cálculos"""
        logger.info("Validando resultados dos cálculos...")
        
        # Verificar valores zerados ou inválidos
        zero_values = (df['VALOR_TOTAL_VR'] == 0).sum()
        if zero_values > 0:
            logger.warning(f"Encontrados {zero_values} funcionários com valor zero")
        
        # Verificar consistência dos percentuais
        inconsistencies = 0
        for idx, row in df.iterrows():
            total = row['VALOR_TOTAL_VR']
            empresa = row['CUSTO_EMPRESA'] 
            colaborador = row['DESCONTO_PROFISSIONAL']
            
            if abs(total - (empresa + colaborador)) > 0.01:
                inconsistencies += 1
                logger.warning(f"MATRICULA {row['MATRICULA']}: Inconsistência nos valores")
        
        if inconsistencies > 0:
            logger.warning(f"Encontradas {inconsistencies} inconsistências nos cálculos")
        else:
            logger.info("Todos os cálculos estão consistentes")
    
    def _calculate_monthly_workdays(self, state: VRState, base_dias_uteis_df: pd.DataFrame = None) -> int:
        """Calcula dias úteis do mês de referência"""
        logger.info("Calculando dias úteis do mês...")
        
        month_year = state.get("month_year", "05/2025")
        try:
            month, year = map(int, month_year.split('/'))
        except:
            logger.warning(f"Formato de mês/ano inválido: {month_year}. Usando padrão.")
            month, year = 5, 2025
        
        workdays = self.business_rules.calculate_workdays(month, year, base_dias_uteis_df)
        logger.info(f"Dias úteis calculados para {month:02d}/{year}: {workdays}")
        
        return workdays
    
    def _calculate_final_statistics(self, df: pd.DataFrame, state: VRState):
        """Calcula estatísticas finais do processamento"""
        logger.info("Calculando estatísticas finais...")
        
        # Totais gerais
        total_employees = len(df)
        total_value_company = df['CUSTO_EMPRESA'].sum()
        total_value_employee = df['DESCONTO_PROFISSIONAL'].sum() 
        total_value_general = df['VALOR_TOTAL_VR'].sum()
        
        # Estatísticas por sindicato
        union_stats = {}
        if 'Sindicato' in df.columns:
            for union in df['Sindicato'].dropna().unique():
                if pd.notna(union):
                    union_df = df[df['Sindicato'] == union]
                    union_stats[str(union)[:50]] = {  # Limitar tamanho da chave
                        'employees': len(union_df),
                        'total_value': round(union_df['VALOR_TOTAL_VR'].sum(), 2)
                    }
        
        # Estatísticas por tipo de cálculo
        calc_type_stats = {}
        if 'TIPO_CALCULO' in df.columns:
            calc_type_stats = df['TIPO_CALCULO'].value_counts().to_dict()
        
        # Atualizar estado com estatísticas
        state.update({
            'total_calculated_employees': total_employees,
            'total_company_cost': round(total_value_company, 2),
            'total_employee_discount': round(total_value_employee, 2),
            'total_vr_value': round(total_value_general, 2),
            'union_calculation_stats': union_stats,
            'calculation_type_stats': calc_type_stats,
            'average_vr_value': round(df['VALOR_TOTAL_VR'].mean(), 2) if len(df) > 0 else 0,
            'average_workdays': round(df['DIAS_CALCULO'].mean(), 1) if len(df) > 0 else 0
        })
        
        # Logs de resumo detalhado
        logger.info(f"=== RESUMO DOS CÁLCULOS FINAIS ===")
        logger.info(f"Funcionários processados: {total_employees}")
        logger.info(f"Valor total VR: R$ {total_value_general:,.2f}")
        logger.info(f"Custo empresa (80%): R$ {total_value_company:,.2f}")
        logger.info(f"Desconto colaborador (20%): R$ {total_value_employee:,.2f}")
        logger.info(f"Valor médio por funcionário: R$ {df['VALOR_TOTAL_VR'].mean():.2f}")
        logger.info(f"Média de dias calculados: {df['DIAS_CALCULO'].mean():.1f}")
        logger.info(f"Distribuição por tipo de cálculo: {calc_type_stats}")