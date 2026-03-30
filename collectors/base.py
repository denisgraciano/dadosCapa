"""
collectors/base.py — Classes base e exceções para coletores de dados judiciais.
Define contratos e erros padrão para integração com diferentes fontes.
"""

from abc import ABC, abstractmethod
from datetime import date
from typing import Optional


# ─────────────────────────────────────────────────────────────
# EXCEÇÕES PADRÃO
# ─────────────────────────────────────────────────────────────

class CollectorError(Exception):
    """Erro base para coletores."""
    pass


class CollectorUnavailableError(CollectorError):
    """Erro quando o serviço externo está indisponível."""
    pass


class CaptchaRequiredError(CollectorError):
    """Erro quando um CAPTCHA impede a coleta."""
    pass


# ─────────────────────────────────────────────────────────────
# CLASSE BASE (CONTRATO)
# ─────────────────────────────────────────────────────────────

class BaseCollector(ABC):
    """
    Interface base para todos os coletores.

    Cada collector (Datajud, Scraper, etc.) deve implementar o método `collect`.
    """

    def __init__(self, ultima_mov: Optional[date] = None) -> None:
        self.ultima_mov = ultima_mov

    @abstractmethod
    async def collect(self, cnj: str):
        """
        Coleta os dados do processo a partir de um CNJ.

        Deve retornar um objeto do tipo `DadosCapa`.
        """
        pass