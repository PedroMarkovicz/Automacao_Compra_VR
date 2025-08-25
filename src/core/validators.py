# src/core/validators.py
"""
Validadores de Dados - Data Validators

Validacoes especificas para dados do sistema VR
"""

import pandas as pd
import re
from typing import List, Dict, Tuple

class DataValidators:
    """Classe com validadores de dados para o sistema VR"""
    
    @staticmethod
    def validate_required_columns(df: pd.DataFrame, required_cols: List[str], 
                                file_name: str = "") -> Tuple[bool, List[str]]:
        """
        Valida se DataFrame possui colunas obrigatorias
        
        Args:
            df: DataFrame a ser validado
            required_cols: Lista de colunas obrigatorias
            file_name: Nome do arquivo (para logs)
            
        Returns:
            Tuple[bool, List[str]]: (is_valid, missing_columns)
        """
        missing_cols = [col for col in required_cols if col not in df.columns]
        return len(missing_cols) == 0, missing_cols
    
    @staticmethod
    def validate_matricula(matricula_series: pd.Series) -> Tuple[bool, List[str]]:
        """
        Valida formato de matriculas
        
        Args:
            matricula_series: Serie com matriculas
            
        Returns:
            Tuple[bool, List[str]]: (is_valid, invalid_matriculas)
        """
        invalid_matriculas = []
        
        for matricula in matricula_series:
            if pd.isna(matricula):
                invalid_matriculas.append("Matricula vazia")
                continue
                
            matricula_str = str(matricula).strip()
            
            # Verificar se e numerica e tem tamanho adequado
            if not matricula_str.isdigit() or len(matricula_str) < 4:
                invalid_matriculas.append(matricula_str)
        
        return len(invalid_matriculas) == 0, invalid_matriculas
    
    @staticmethod
    def validate_dates(date_series: pd.Series, column_name: str = "") -> Tuple[bool, List[str]]:
        """
        Valida formato de datas
        
        Args:
            date_series: Serie com datas
            column_name: Nome da coluna (para logs)
            
        Returns:
            Tuple[bool, List[str]]: (is_valid, invalid_dates)
        """
        invalid_dates = []
        
        for idx, date_val in enumerate(date_series):
            if pd.isna(date_val):
                continue  # Datas vazias sao permitidas
                
            try:
                # Tentar converter para datetime
                if isinstance(date_val, str):
                    pd.to_datetime(date_val, format='%d/%m/%Y')
                elif not isinstance(date_val, (pd.Timestamp)):
                    invalid_dates.append(f"Linha {idx+1}: {date_val}")
                    
            except (ValueError, TypeError):
                invalid_dates.append(f"Linha {idx+1}: {date_val}")
        
        return len(invalid_dates) == 0, invalid_dates
    
    @staticmethod
    def validate_numeric_values(value_series: pd.Series, column_name: str = "", 
                               min_value: float = None, max_value: float = None) -> Tuple[bool, List[str]]:
        """
        Valida valores numericos
        
        Args:
            value_series: Serie com valores
            column_name: Nome da coluna
            min_value: Valor minimo permitido
            max_value: Valor maximo permitido
            
        Returns:
            Tuple[bool, List[str]]: (is_valid, invalid_values)
        """
        invalid_values = []
        
        for idx, value in enumerate(value_series):
            if pd.isna(value):
                continue
                
            try:
                numeric_value = float(value)
                
                if min_value is not None and numeric_value < min_value:
                    invalid_values.append(f"Linha {idx+1}: {value} < {min_value}")
                    
                if max_value is not None and numeric_value > max_value:
                    invalid_values.append(f"Linha {idx+1}: {value} > {max_value}")
                    
            except (ValueError, TypeError):
                invalid_values.append(f"Linha {idx+1}: {value} nao e numerico")
        
        return len(invalid_values) == 0, invalid_values
    
    @staticmethod
    def validate_data_consistency(df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """
        Valida consistencia geral dos dados
        
        Args:
            df: DataFrame a ser validado
            
        Returns:
            Tuple[bool, List[str]]: (is_valid, consistency_errors)
        """
        errors = []
        
        # Verificar duplicatas de matricula
        if 'MATRICULA' in df.columns:
            duplicates = df[df.duplicated('MATRICULA', keep=False)]['MATRICULA'].tolist()
            if duplicates:
                errors.append(f"Matriculas duplicadas encontradas: {list(set(duplicates))}")
        
        # Verificar registros completamente vazios
        empty_rows = df.isnull().all(axis=1).sum()
        if empty_rows > 0:
            errors.append(f"Encontradas {empty_rows} linhas completamente vazias")
        
        return len(errors) == 0, errors