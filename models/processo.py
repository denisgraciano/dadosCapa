"""
models/processo.py — Modelos Pydantic que representam os dados de um processo judicial.
Esses modelos são usados como DTOs entre coletores, pipeline e repositórios.
"""

from datetime import date
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


PoloType = Literal["autor", "reu", "advogado", "outro"]
InstanciaType = Literal["1grau", "2grau", "stj", "stf", "tst"]
FonteType = Literal["datajud", "scraping"]


class Parte(BaseModel):
    polo: PoloType
    nome_tribunal: str = Field(..., max_length=300)
    documento: str | None = Field(default=None, max_length=20)


class Movimentacao(BaseModel):
    data_mov: date
    codigo_mov: str | None = Field(default=None, max_length=20)
    descricao: str
    complemento: str | None = None


class DadosCapa(BaseModel):
    """Dados completos da capa do processo, coletados de qualquer fonte."""

    cnj: str = Field(..., max_length=20)
    tribunal: str = Field(..., max_length=20)
    classe: str | None = Field(default=None, max_length=200)
    assunto: str | None = Field(default=None, max_length=500)
    valor_causa: Decimal | None = None
    vara: str | None = Field(default=None, max_length=200)
    juiz: str | None = Field(default=None, max_length=200)
    instancia: InstanciaType | None = None
    status: str | None = Field(default=None, max_length=100)
    data_distribuicao: date | None = None
    fonte: FonteType
    raw_json: dict[str, Any] | None = None
    ultima_mov: date | None = None

    partes: list[Parte] = Field(default_factory=list)
    movimentacoes: list[Movimentacao] = Field(default_factory=list)

    @field_validator("vara", "juiz", "classe", "status", mode="before")
    @classmethod
    def _truncar_campo_200(cls, v: str | None) -> str | None:
        """
        Trunca campos limitados a 200 chars antes da validação Pydantic.
        Evita ValidationError quando portais retornam texto concatenado longo.
        Pega apenas a primeira linha significativa do valor.
        """
        if not v or not isinstance(v, str):
            return v
        # Pega só a primeira linha não vazia (portais às vezes concatenam tudo)
        primeira_linha = next(
            (linha.strip() for linha in v.split("\n") if linha.strip()), v
        ).strip()
        if len(primeira_linha) > 200:
            from loguru import logger
            logger.debug(f"Campo truncado de {len(primeira_linha)} para 200 chars: {primeira_linha[:50]!r}...")
        return primeira_linha[:200] if primeira_linha else None

    @field_validator("assunto", mode="before")
    @classmethod
    def _truncar_campo_500(cls, v: str | None) -> str | None:
        if not v or not isinstance(v, str):
            return v
        primeira_linha = next(
            (linha.strip() for linha in v.split("\n") if linha.strip()), v
        ).strip()
        return primeira_linha[:500] if primeira_linha else None