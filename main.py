"""
main.py — Entrypoint CLI do process-monitor.

Comandos disponíveis:
  python main.py sync                          → sincroniza todos os CNJs do .env
  python main.py sync --cnj 10022541820258110018  → sincroniza um CNJ específico
"""

import asyncio
import sys
from pathlib import Path

from loguru import logger

from config import settings
from pipeline.orquestrador import Orquestrador


def _configurar_logging() -> None:
    """Configura Loguru com dois sinks: console colorido e arquivo rotativo."""
    logger.remove()  # Remove o handler padrão

    # Console — DEBUG com cores
    logger.add(
        sys.stderr,
        level="DEBUG",
        colorize=True,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> │ "
            "<level>{level:<8}</level> │ "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>"
        ),
    )

    # Arquivo rotativo — INFO, retido por 30 dias
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    logger.add(
        log_dir / "process_monitor_{time:YYYY-MM-DD}.log",
        level="INFO",
        rotation="00:00",       # Novo arquivo a meia-noite
        retention="30 days",
        encoding="utf-8",
        format="{time:YYYY-MM-DD HH:mm:ss} │ {level:<8} │ {name}:{function}:{line} - {message}",
    )


async def _run_sync(cnjs: list[str]) -> None:
    """Instancia o orquestrador e executa o pipeline de sync."""
    orquestrador = Orquestrador(
        db_config=settings.get_db_config(),
        partes_sistema=settings.get_partes_sistema_list(),
    )
    await orquestrador.executar(cnjs)


def _parse_args() -> tuple[str, str | None]:
    """Parse mínimo de argumentos CLI sem dependências externas."""
    args = sys.argv[1:]

    if not args:
        print("Uso: python main.py sync [--cnj <numero_cnj>]")
        sys.exit(1)

    comando = args[0]

    cnj_especifico: str | None = None
    if "--cnj" in args:
        idx = args.index("--cnj")
        if idx + 1 >= len(args):
            print("Erro: --cnj requer um valor.")
            sys.exit(1)
        cnj_especifico = args[idx + 1].strip()

    return comando, cnj_especifico


def main() -> None:
    _configurar_logging()
    comando, cnj_especifico = _parse_args()

    if comando != "sync":
        logger.error(f"Comando desconhecido: '{comando}'. Use: sync")
        sys.exit(1)

    if cnj_especifico:
        cnjs = [cnj_especifico]
        logger.info(f"Modo: sync de CNJ único → {cnj_especifico}")
    else:
        cnjs = settings.get_cnjs_list()
        logger.info(f"Modo: sync completo → {len(cnjs)} CNJ(s) do .env")

    asyncio.run(_run_sync(cnjs))


if __name__ == "__main__":
    main()
