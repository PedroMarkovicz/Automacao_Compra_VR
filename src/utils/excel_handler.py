# src/utils/excel_handler.py
import pandas as pd
from pathlib import Path
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


class ExcelHandler:
    @staticmethod
    def read_excel_file(
        filepath: Path, sheet_name: Optional[str] = None
    ) -> pd.DataFrame:
        """Lê arquivo Excel com tratamento de erros"""
        try:
            # Se sheet_name não especificado, usar primeira aba
            if sheet_name is None:
                # Ler primeiro para verificar se há múltiplas abas
                excel_file = pd.ExcelFile(filepath)
                if len(excel_file.sheet_names) > 1:
                    # Se múltiplas abas, usar a primeira
                    sheet_name = excel_file.sheet_names[0]
                    logger.info(f"Usando aba '{sheet_name}' do arquivo {filepath.name}")
                excel_file.close()
            
            df = pd.read_excel(filepath, sheet_name=sheet_name)
            
            # Se ainda retornou um dict (não deveria acontecer), pegar primeira entrada
            if isinstance(df, dict):
                first_key = list(df.keys())[0]
                df = df[first_key]
                logger.info(f"Extraído aba '{first_key}' de {filepath.name}")
            
            logger.info(f"Arquivo {filepath.name} carregado: {len(df)} registros")
            return df
        except Exception as e:
            logger.error(f"Erro ao ler {filepath}: {e}")
            raise

    @staticmethod
    def read_all_input_files(input_dir: Path) -> Dict[str, pd.DataFrame]:
        """Lê todos os 11 arquivos de entrada"""
        files_map = {
            "ativos": "ATIVOS.xlsx",
            "ferias": "FERIAS.xlsx",
            "desligados": "DESLIGADOS.xlsx",
            "admissao": "ADMISSÃO ABRIL.xlsx",
            "afastamentos": "AFASTAMENTOS.xlsx",
            "aprendiz": "APRENDIZ.xlsx",
            "estagio": "ESTAGIO.xlsx",
            "exterior": "EXTERIOR.xlsx",
            "base_sindicato": "Base sindicato x valor.xlsx",
            "base_dias_uteis": "Base dias uteis.xlsx",
            "vr_mensal": "VR MENSAL 05.2025.xlsx",
        }

        data = {}
        for key, filename in files_map.items():
            filepath = input_dir / filename
            if filepath.exists():
                data[key] = ExcelHandler.read_excel_file(filepath)
            else:
                logger.warning(f"Arquivo não encontrado: {filename}")

        return data
