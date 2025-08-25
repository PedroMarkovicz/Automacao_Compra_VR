# run.py
"""
Script de Execução Principal - VR Automation Runner

Ponto de entrada para executar o pipeline completo de processamento VR
Configura logging, carrega variaveis de ambiente e executa o workflow
"""

import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path
import os

# Adicionar src ao path
sys.path.append(str(Path(__file__).parent))

from src.graph.workflow import VRWorkflow
from src.graph.state import VRState
from src.config import Config


# Configurar logging
def setup_logging(debug_mode=False):
    """Configura o sistema de logging"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    log_level = logging.DEBUG if debug_mode else logging.INFO
    log_file = log_dir / f"vr_processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )
    return logging.getLogger(__name__)


def print_header():
    """Imprime cabecalho do sistema"""
    print("=" * 60)
    print(" " * 15 + "SISTEMA DE AUTOMACAO VR/VA")
    print(" " * 10 + "Processamento Mensal de Beneficios")
    print("=" * 60)
    print()


def print_progress(stage: str, step: int, total: int = 5):
    """Imprime progresso do processamento"""
    progress = "=" * (step * 2) + "-" * ((total - step) * 2)
    print(f"\r  [{step}/{total}] [{progress}] {stage}...", end="", flush=True)
    if step == total:
        print()  # Nova linha ao final


def print_results(state: VRState):
    """Imprime resumo dos resultados"""
    print("\n" + "=" * 60)
    print("RESUMO DO PROCESSAMENTO")
    print("=" * 60)

    if state.get("success"):
        print("OK Status: SUCESSO")
    else:
        print("X Status: FALHA")

    print(f"Total de funcionarios: {state.get('total_employees', 0)}")
    print(f"Elegiveis: {state.get('eligible_employees', 0)}")
    print(f"Excluidos: {state.get('excluded_employees', 0)}")

    if state.get("output_file"):
        print(f"\n📁 Arquivo gerado: {state['output_file']}")

    if state.get("errors"):
        print(f"\nErros encontrados: {len(state['errors'])}")
        for error in state["errors"][:3]:  # Mostrar ate 3 erros
            print(f"   - {error['stage']}: {error['error'][:50]}...")

    if state.get("warnings"):
        print(f"\nAvisos: {len(state['warnings'])}")
        for warning in state["warnings"][:3]:  # Mostrar ate 3 avisos
            if isinstance(warning, dict):
                print(
                    f"   - {warning.get('stage', 'N/A')}: {warning.get('message', 'N/A')}"
                )

    print("=" * 60)


async def monitor_progress(state: VRState, stage_map: dict):
    """Monitora o progresso do workflow"""
    stages = [
        "ingestion_complete",
        "consolidation_complete",
        "validation_complete",
        "calculation_complete",
        "report_complete",
    ]

    current_stage = 0
    for i, stage in enumerate(stages):
        if state.get("processing_stage") == stage:
            current_stage = i + 1
            stage_name = stage_map.get(stage, stage)
            print_progress(stage_name, current_stage)
            break


async def run_workflow(month_year: str = None, debug: bool = False):
    """Executa o workflow de processamento VR"""

    # Configurações já centralizadas em src.config

    # Configurar logging
    logger = setup_logging(debug)

    # Estado inicial
    initial_state: VRState = {
        "raw_files": {},
        "consolidated_df": None,
        "validated_df": None,
        "calculated_df": None,
        "final_report": None,
        "month_year": month_year or Config.PROCESSING_MONTH,
        "total_employees": 0,
        "eligible_employees": 0,
        "excluded_employees": 0,
        "errors": [],
        "warnings": [],
        "processing_stage": "initialized",
        "success": False,
        "output_file": None,
    }

    stage_map = {
        "ingestion_complete": "Ingestao de dados",
        "consolidation_complete": "Consolidacao",
        "validation_complete": "Validacao",
        "calculation_complete": "Calculo de beneficios",
        "report_complete": "Geracao de relatorio",
    }

    try:
        print_header()
        print(f">> Iniciando processamento para {initial_state['month_year']}")
        print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")

        # Validar diretórios
        input_path = Config.INPUT_PATH
        if not input_path.exists():
            print(f"X Erro: Diretorio de entrada nao encontrado: {input_path}")
            return 1

        # Criar e compilar workflow
        logger.info("Criando workflow...")
        print("Configurando pipeline...")
        workflow = VRWorkflow()
        app = workflow.compile()

        # Executar workflow
        logger.info("Executando pipeline de processamento...")
        print("\nProcessando:")

        # Simular progresso visual
        stages_names = [
            "Ingestao de dados",
            "Consolidacao",
            "Validacao",
            "Calculo de beneficios",
            "Geracao de relatorio",
        ]

        # Executar com callbacks para progresso
        for i, stage_name in enumerate(stages_names, 1):
            print_progress(stage_name, i)
            await asyncio.sleep(0.1)  # Pequena pausa para visualizacao

        # Executar workflow real
        result = await app.ainvoke(initial_state)

        print()  # Nova linha após progresso

        # Mostrar resultados
        print_results(result)

        if result["success"]:
            logger.info("Processamento concluido com sucesso")
            print("\nOK Processamento finalizado com sucesso!")
            return 0
        else:
            logger.error("Processamento concluido com erros")
            print("\nX Processamento finalizado com erros.")
            return 1

    except KeyboardInterrupt:
        print("\n\nProcessamento interrompido pelo usuario")
        logger.warning("Processamento interrompido")
        return 2

    except FileNotFoundError as e:
        print(f"\n\nX Erro: Arquivo nao encontrado - {str(e)}")
        logger.error(f"Arquivo nao encontrado: {str(e)}")
        return 3

    except Exception as e:
        print(f"\n\nX Erro fatal: {str(e)}")
        logger.error(f"Erro fatal no processamento: {str(e)}", exc_info=True)
        if debug:
            import traceback

            traceback.print_exc()
        return 4


def validate_environment():
    """Valida o ambiente antes de executar"""
    errors = []

    # Verificar diretorios
    input_path = Path(os.getenv("INPUT_PATH", "data/input"))
    if not input_path.exists():
        errors.append(f"Diretorio de entrada nao existe: {input_path}")

    # Verificar arquivos essenciais
    required_files = [
        "ATIVOS.xlsx",
        "Base sindicato x valor.xlsx",
        "Base dias uteis.xlsx",
    ]
    for file in required_files:
        if not (input_path / file).exists():
            errors.append(f"Arquivo obrigatorio nao encontrado: {file}")

    if errors:
        print("X Erros de validacao do ambiente:")
        for error in errors:
            print(f"   - {error}")
        return False

    return True


def main():
    """Funcao principal"""
    import argparse

    # Parser de argumentos
    parser = argparse.ArgumentParser(
        description="Sistema de Automacao VR/VA - Processamento de Beneficios",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
 python run.py                    # Processa o mês padrão (05/2025)
 python run.py --month 06/2025    # Processa junho de 2025
 python run.py --debug            # Executa em modo debug
 python run.py --validate         # Apenas valida o ambiente
       """,
    )

    parser.add_argument(
        "--month",
        type=str,
        help="Mes/Ano para processamento (formato: MM/YYYY)",
        default=None,
    )

    parser.add_argument(
        "--debug", action="store_true", help="Ativar modo debug com logs detalhados"
    )

    parser.add_argument(
        "--validate",
        action="store_true",
        help="Apenas valida o ambiente sem executar o processamento",
    )

    parser.add_argument(
        "--version", action="version", version="VR Automation System v1.0.0"
    )

    args = parser.parse_args()

    # Modo validacao
    if args.validate:
        print("Validando ambiente...")
        if validate_environment():
            print("OK Ambiente valido e pronto para execucao!")
            sys.exit(0)
        else:
            print("X Ambiente invalido. Corrija os erros acima.")
            sys.exit(1)

    # Validar ambiente antes de executar
    if not validate_environment():
        print("\nExecute com --validate para mais detalhes")
        sys.exit(1)

    # Executar workflow
    try:
        exit_code = asyncio.run(run_workflow(args.month, args.debug))
        sys.exit(exit_code)
    except Exception as e:
        print(f"\nX Erro ao executar: {str(e)}")
        if args.debug:
            import traceback

            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
