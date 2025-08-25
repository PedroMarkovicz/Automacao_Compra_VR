# src/core/rules.py
"""
Regras de Negocio - VR Business Rules

Define as regras de validacao e calculo de beneficios VR/VA
baseadas nas convencoes coletivas e politicas da empresa
"""

import pandas as pd
from typing import List, Dict, Any, Tuple
from datetime import datetime, date
from src.config import Config

class VRBusinessRules:
    """Classe que encapsula todas as regras de negocio para calculo VR"""
    
    def __init__(self):
        self.config = Config()
        # Feriados nacionais fixos (simplificado)
        self.br_holidays = {
            (1, 1): "Ano Novo",
            (4, 21): "Tiradentes",
            (5, 1): "Dia do Trabalhador",
            (9, 7): "Independencia",
            (10, 12): "Nossa Senhora Aparecida",
            (11, 2): "Finados",
            (11, 15): "Proclamacao da Republica",
            (12, 25): "Natal"
        }
        
    def should_exclude_employee(self, employee_data: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Verifica se funcionario deve ser excluido do beneficio baseado em regras atualizadas
        
        Returns:
            tuple: (should_exclude: bool, reason: str)
        """
        matricula = employee_data.get('MATRICULA', 'N/A')
        
        # 1. Verificar cargos excluidos (diretores, etc)
        cargo = str(employee_data.get('TITULO DO CARGO', '')).upper()
        for excluded_position in self.config.EXCLUDED_POSITIONS:
            if excluded_position in cargo:
                return True, f"Cargo excluido: {cargo}"
        
        # 2. Verificar flags de exclusao especificas
        exclusion_flags = {
            'IS_ESTAGIARIO': 'Estagiario',
            'IS_APRENDIZ': 'Aprendiz',
            'IS_AFASTADO': 'Afastado (Licenca/Auxilio Doenca)',
            'IS_EXTERIOR': 'Trabalha no exterior'
        }
        
        for flag, reason in exclusion_flags.items():
            if employee_data.get(flag, False):
                return True, f"{reason} (MATRICULA: {matricula})"
        
        # 3. Verificar situacao de trabalho
        situacao = str(employee_data.get('DESC. SITUACAO', '')).upper()
        situacoes_excluidas = [
            'LICENCA MATERNIDADE',
            'AUXILIO DOENCA', 
            'ATESTADO',
            'AFASTADO'
        ]
        
        if situacao in situacoes_excluidas:
            return True, f"Situacao excluida: {situacao}"
        
        # 4. Verificar desligamentos (sera tratado em validacao separada)
        # IS_DESLIGADO sera processado pela regra de data de desligamento
        
        return False, ""
    
    def calculate_workdays(self, month: int, year: int, base_dias_uteis: pd.DataFrame = None) -> int:
        """
        Calcula dias uteis do mes considerando feriados e sindicatos
        
        Args:
            month: Mes de referencia
            year: Ano de referencia  
            base_dias_uteis: DataFrame com dias uteis pre-calculados por sindicato
            
        Returns:
            int: Numero de dias uteis (valor padrao, especifico por sindicato sera usado em outro local)
        """
        if base_dias_uteis is not None and not base_dias_uteis.empty:
            # Usar dados da planilha - pegar valor medio se houver multiplos sindicatos
            if 'DIAS_UTEIS' in base_dias_uteis.columns:
                dias_values = base_dias_uteis['DIAS_UTEIS'].dropna()
                if not dias_values.empty:
                    # Retornar valor mais comum ou media
                    return int(dias_values.mode().iloc[0] if not dias_values.mode().empty else dias_values.mean())
        
        # Calcular dinamicamente se nao houver planilha
        return self._calculate_workdays_dynamic(month, year)
    
    def get_workdays_by_union(self, sindicato: str, base_dias_uteis: pd.DataFrame = None) -> int:
        """
        Obtem dias uteis especificos por sindicato
        
        Args:
            sindicato: Nome do sindicato
            base_dias_uteis: DataFrame com dias uteis por sindicato
            
        Returns:
            int: Numero de dias uteis para o sindicato
        """
        if base_dias_uteis is not None and not base_dias_uteis.empty:
            if 'SINDICATO' in base_dias_uteis.columns and 'DIAS_UTEIS' in base_dias_uteis.columns:
                # Buscar correspondencia exata primeiro
                exact_match = base_dias_uteis[base_dias_uteis['SINDICATO'] == sindicato]
                if not exact_match.empty:
                    return int(exact_match.iloc[0]['DIAS_UTEIS'])
                
                # Buscar correspondencia parcial
                partial_match = base_dias_uteis[
                    base_dias_uteis['SINDICATO'].str.contains(sindicato, case=False, na=False)
                ]
                if not partial_match.empty:
                    return int(partial_match.iloc[0]['DIAS_UTEIS'])
        
        # Valores padrao por regiao (baseado na analise)
        if 'SP' in sindicato.upper():
            return 22
        elif 'RS' in sindicato.upper():
            return 21  
        elif 'PR' in sindicato.upper():
            return 22
        elif 'RJ' in sindicato.upper():
            return 21
        
        return self.config.DEFAULT_WORKDAYS
    
    def _calculate_workdays_dynamic(self, month: int, year: int) -> int:
        """Calcula dias uteis dinamicamente"""
        start_date = date(year, month, 1)
        
        # Ultimo dia do mes
        if month == 12:
            end_date = date(year + 1, 1, 1)
        else:
            end_date = date(year, month + 1, 1)
            
        workdays = 0
        current_date = start_date
        
        while current_date < end_date:
            # Segunda a sexta (0-4) e nao e feriado
            is_holiday = (current_date.month, current_date.day) in self.br_holidays
            if current_date.weekday() < 5 and not is_holiday:
                workdays += 1
            
            # Incrementar data corretamente
            from datetime import timedelta
            current_date = current_date + timedelta(days=1)
            
        return workdays
    
    def get_daily_value_by_union(self, sindicato: str, base_sindicato_df: pd.DataFrame = None) -> float:
        """
        Obtem o valor diario do VR baseado no sindicato com mapeamento atualizado
        
        Args:
            sindicato: Nome do sindicato
            base_sindicato_df: DataFrame com valores por sindicato
            
        Returns:
            float: Valor diario do VR
        """
        # Mapeamento atualizado baseado na analise dos dados
        union_value_map = {
            'SINDPD SP': 37.5,  # Sao Paulo
            'SINDPPD RS': 35.0,  # Rio Grande do Sul  
            'SITEPD PR': 35.0,   # Parana
            'SINDPD RJ': 35.0    # Rio de Janeiro
        }
        
        # Buscar por correspondencia parcial no nome
        sindicato_upper = str(sindicato).upper()
        for key, value in union_value_map.items():
            if key in sindicato_upper:
                return value
        
        # Tentar usar DataFrame se fornecido
        if base_sindicato_df is not None and not base_sindicato_df.empty:
            if 'SINDICATO' in base_sindicato_df.columns and 'VALOR_DIA' in base_sindicato_df.columns:
                # Buscar correspondencia exata primeiro
                exact_match = base_sindicato_df[base_sindicato_df['SINDICATO'] == sindicato]
                if not exact_match.empty:
                    return float(exact_match.iloc[0]['VALOR_DIA'])
                
                # Buscar correspondencia parcial
                partial_match = base_sindicato_df[
                    base_sindicato_df['SINDICATO'].str.contains(sindicato, case=False, na=False)
                ]
                if not partial_match.empty:
                    return float(partial_match.iloc[0]['VALOR_DIA'])
        
        # Valor padrao
        return self.config.DEFAULT_DAILY_VALUE
    
    def calculate_benefit_values(self, daily_value: float, workdays: int) -> Dict[str, float]:
        """
        Calcula os valores do beneficio (empresa e colaborador)
        
        Args:
            daily_value: Valor diario do VR
            workdays: Dias uteis do mes
            
        Returns:
            Dict com valores calculados
        """
        valor_total = daily_value * workdays
        valor_empresa = valor_total * self.config.COMPANY_PERCENTAGE
        valor_colaborador = valor_total * self.config.EMPLOYEE_PERCENTAGE
        
        return {
            'VALOR_DIA': daily_value,
            'DIAS_UTEIS': workdays,
            'VALOR_TOTAL': valor_total,
            'VALOR_EMPRESA': valor_empresa,
            'VALOR_COLABORADOR': valor_colaborador
        }
    
    def is_on_vacation(self, employee_data: Dict[str, Any], cutoff_date: date) -> bool:
        """
        Verifica se funcionario esta de ferias no periodo
        
        Args:
            employee_data: Dados do funcionario
            cutoff_date: Data de corte para verificacao
            
        Returns:
            bool: True se estiver de ferias
        """
        inicio_ferias = employee_data.get('INICIO_FERIAS')
        fim_ferias = employee_data.get('FIM_FERIAS')
        
        if pd.isna(inicio_ferias) or pd.isna(fim_ferias):
            return False
            
        try:
            if isinstance(inicio_ferias, str):
                inicio_ferias = datetime.strptime(inicio_ferias, '%d/%m/%Y').date()
            if isinstance(fim_ferias, str):
                fim_ferias = datetime.strptime(fim_ferias, '%d/%m/%Y').date()
                
            return inicio_ferias <= cutoff_date <= fim_ferias
            
        except (ValueError, TypeError):
            return False
    
    def get_cutoff_date(self, month_year: str) -> date:
        """
        Obtem a data de corte baseada no mes de referencia
        
        Args:
            month_year: Mes/ano no formato MM/YYYY
            
        Returns:
            date: Data de corte
        """
        try:
            month, year = map(int, month_year.split('/'))
            return date(year, month, self.config.CUTOFF_DAY)
        except:
            return date.today()
    
    def should_exclude_by_dismissal_date(self, dismissal_date: date, cutoff_day: int = 15) -> Tuple[bool, str]:
        """
        Verifica se funcionario deve ser excluido baseado na data de desligamento
        Regra: Se comunicado ate dia 15, nao considera para pagamento
        
        Args:
            dismissal_date: Data de demissao
            cutoff_day: Dia de corte (padrao: 15)
            
        Returns:
            tuple: (should_exclude, calculation_type)
        """
        if dismissal_date.day <= cutoff_day:
            return True, f"Demissao comunicada ate dia {cutoff_day}"
        else:
            return False, "PROPORCIONAL"  # Pagamento proporcional
    
    def calculate_vacation_adjustment(self, base_days: int, vacation_days: int) -> int:
        """
        Calcula ajuste de dias para funcionarios em ferias
        
        Args:
            base_days: Dias uteis base
            vacation_days: Dias de ferias
            
        Returns:
            int: Dias ajustados
        """
        # Regra: descontar dias de ferias dos dias uteis
        adjusted_days = max(0, base_days - vacation_days)
        return adjusted_days