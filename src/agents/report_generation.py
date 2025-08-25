# src/agents/report_generation.py
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
import os
from src.graph.state import VRState

logger = logging.getLogger(__name__)


class ReportGenerationAgent:
    def __init__(self, output_path: str = None):
        self.output_path = Path(output_path or os.getenv("OUTPUT_PATH", "data/output"))
        
    def execute(self, state: VRState) -> VRState:
        """Gera relatório final no formato exato especificado no descricao.md"""
        logger.info("Executando ReportGenerationAgent - gerando formato final...")
        
        try:
            # Obter DataFrame calculado
            df = self._extract_dataframe_from_state(state)
            
            if df is None or df.empty:
                raise ValueError("Nenhum DataFrame válido encontrado no state")
            
            # Transformar para formato final especificado
            final_df = self._transform_to_final_format(df, state)
            
            # Gerar nome do arquivo com timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{timestamp}_vr_final_report.xlsx"
            filepath = self.output_path / filename
            
            # Salvar Excel no formato correto
            self._save_formatted_excel(final_df, filepath)
            
            # Atualizar estado
            state["final_report"] = final_df
            state["output_file"] = str(filepath)
            state["processing_stage"] = "report_complete"
            state["success"] = True
            
            logger.info(f"Relatório final gerado: {filepath}")
            logger.info(f"Total de registros: {len(final_df)}")
            logger.info(f"Formato: {list(final_df.columns)}")
            
            return state
            
        except Exception as e:
            logger.error(f"Erro na geracao do relatorio: {str(e)}")
            if "errors" not in state:
                state["errors"] = []
            state["errors"].append({"stage": "report_generation", "error": str(e)})
            state["success"] = False
            state["processing_stage"] = "report_failed"
            return state
    
    def _extract_dataframe_from_state(self, state: VRState) -> pd.DataFrame:
        """Extrai DataFrame do state de forma robusta"""
        
        # Tentar calculated_df primeiro
        if "calculated_df" in state:
            df_data = state["calculated_df"]
            df = self._convert_to_dataframe(df_data, "calculated_df")
            if df is not None:
                return df
        
        # Fallback para validated_df
        if "validated_df" in state:
            df_data = state["validated_df"]  
            df = self._convert_to_dataframe(df_data, "validated_df")
            if df is not None:
                return df
        
        # Fallback para consolidated_df
        if "consolidated_df" in state:
            df_data = state["consolidated_df"]
            df = self._convert_to_dataframe(df_data, "consolidated_df")
            if df is not None:
                return df
                
        return None
    
    def _convert_to_dataframe(self, data, source_name: str) -> pd.DataFrame:
        """Converte dados para DataFrame de forma segura"""
        try:
            if data is None:
                return None
                
            if isinstance(data, pd.DataFrame):
                logger.info(f"Usando {source_name}: DataFrame direto com {len(data)} registros")
                return data
                
            elif isinstance(data, dict):
                # LangGraph serialization - procurar pela chave do DataFrame
                if 'ATIVOS' in data:
                    df = data['ATIVOS']
                    if isinstance(df, pd.DataFrame):
                        logger.info(f"Usando {source_name}['ATIVOS']: {len(df)} registros")
                        return df
                        
                # Tentar outras chaves comuns
                for key in data.keys():
                    value = data[key]
                    if isinstance(value, pd.DataFrame) and len(value) > 0:
                        logger.info(f"Usando {source_name}['{key}']: {len(value)} registros")
                        return value
                        
            return None
            
        except Exception as e:
            logger.warning(f"Erro ao converter {source_name}: {e}")
            return None
    
    def _transform_to_final_format(self, df: pd.DataFrame, state: VRState) -> pd.DataFrame:
        """Transforma DataFrame para o formato final especificado no descricao.md"""
        logger.info("Transformando para formato final especificado...")
        
        # Formato esperado baseado no descricao.md:
        # Matrícula, Admissão, Sindicato do Colaborador, Competência, Dias, 
        # Valor Diário VR, Total, Custo empresa, Desconto profissional, Observações
        
        final_df = pd.DataFrame()
        
        # Garantir que MATRICULA seja o primeiro campo
        if 'MATRICULA' not in df.columns:
            raise ValueError("MATRICULA não encontrada no DataFrame")
            
        final_df['Matricula'] = df['MATRICULA']
        
        # Admissão - usar data de admissão ou data padrão
        if 'DATA_ADMISSAO' in df.columns:
            final_df['Admissão'] = pd.to_datetime(df['DATA_ADMISSAO'], errors='coerce').dt.strftime('%d/%m/%Y')
        elif 'Admissão' in df.columns:
            final_df['Admissão'] = pd.to_datetime(df['Admissão'], errors='coerce').dt.strftime('%d/%m/%Y')
        else:
            # Data padrão se não houver
            final_df['Admissão'] = '01/01/2024'
            
        # Sindicato do Colaborador
        if 'Sindicato' in df.columns:
            final_df['Sindicato do Colaborador'] = df['Sindicato']
        else:
            final_df['Sindicato do Colaborador'] = 'Não informado'
            
        # Competência - mês de referência
        competencia = state.get('month_year', '05/2025')
        try:
            month, year = map(int, competencia.split('/'))
            competencia_formatted = f"01/{month:02d}/{year}"
        except:
            competencia_formatted = "01/05/2025"
        final_df['Competência'] = competencia_formatted
        
        # Dias - dias calculados
        if 'DIAS_CALCULO' in df.columns:
            final_df['Dias'] = df['DIAS_CALCULO'].astype(int)
        elif 'DIAS_UTEIS_SINDICATO' in df.columns:
            final_df['Dias'] = df['DIAS_UTEIS_SINDICATO'].fillna(22).astype(int)
        else:
            final_df['Dias'] = 22
            
        # Valor Diário VR
        if 'VALOR_DIA' in df.columns:
            final_df['VALOR DIÁRIO VR'] = df['VALOR_DIA']
        else:
            # Calcular baseado no total e dias
            total_vr = df.get('VALOR_TOTAL_VR', 0)
            dias = final_df['Dias']
            final_df['VALOR DIÁRIO VR'] = (total_vr / dias).round(2)
            
        # Total - valor total VR
        if 'VALOR_TOTAL_VR' in df.columns:
            final_df['TOTAL'] = df['VALOR_TOTAL_VR']
        elif 'VALOR_TOTAL' in df.columns:
            final_df['TOTAL'] = df['VALOR_TOTAL']
        else:
            final_df['TOTAL'] = final_df['VALOR DIÁRIO VR'] * final_df['Dias']
            
        # Custo empresa (80%)
        if 'CUSTO_EMPRESA' in df.columns:
            final_df['Custo empresa'] = df['CUSTO_EMPRESA']
        elif 'VALOR_EMPRESA' in df.columns:
            final_df['Custo empresa'] = df['VALOR_EMPRESA']
        else:
            final_df['Custo empresa'] = (final_df['TOTAL'] * 0.8).round(2)
            
        # Desconto profissional (20%)
        if 'DESCONTO_PROFISSIONAL' in df.columns:
            final_df['Desconto profissional'] = df['DESCONTO_PROFISSIONAL']
        elif 'VALOR_COLABORADOR' in df.columns:
            final_df['Desconto profissional'] = df['VALOR_COLABORADOR']
        else:
            final_df['Desconto profissional'] = (final_df['TOTAL'] * 0.2).round(2)
            
        # Observações
        if 'OBSERVACOES' in df.columns:
            final_df['OBS GERAL'] = df['OBSERVACOES']
        else:
            final_df['OBS GERAL'] = 'CÁLCULO NORMAL'
        
        # Arredondar valores numéricos
        numeric_columns = ['VALOR DIÁRIO VR', 'TOTAL', 'Custo empresa', 'Desconto profissional']
        for col in numeric_columns:
            if col in final_df.columns:
                final_df[col] = pd.to_numeric(final_df[col], errors='coerce').round(2)
        
        logger.info(f"DataFrame transformado: {len(final_df)} registros com {len(final_df.columns)} colunas")
        logger.info(f"Colunas finais: {list(final_df.columns)}")
        
        return final_df
    
    def _save_formatted_excel(self, df: pd.DataFrame, filepath: Path):
        """Salva Excel formatado conforme especificação"""
        logger.info(f"Salvando arquivo Excel formatado: {filepath}")
        
        # Garantir que o diretório existe
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        # Salvar com formatação
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='VR Mensal', index=False)
            
            # Obter worksheet para formatação
            worksheet = writer.sheets['VR Mensal']
            
            # Ajustar largura das colunas
            column_widths = {
                'A': 15,  # Matricula
                'B': 15,  # Admissão
                'C': 40,  # Sindicato do Colaborador
                'D': 15,  # Competência
                'E': 10,  # Dias
                'F': 18,  # VALOR DIÁRIO VR
                'G': 15,  # TOTAL
                'H': 18,  # Custo empresa
                'I': 20,  # Desconto profissional
                'J': 50   # OBS GERAL
            }
            
            for col_letter, width in column_widths.items():
                worksheet.column_dimensions[col_letter].width = width
                
        logger.info(f"Arquivo Excel salvo com sucesso: {filepath}")