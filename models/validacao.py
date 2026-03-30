"""
models/validacao.py — Modelo Pydantic para o resultado da validação de partes via IA.
"""

from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field, model_validator


StatusValidacao = Literal["confirmado", "revisao", "sem_match", "aprovado_manual"]

# Thresholds centralizados — alterar aqui reflete em todo o sistema
THRESHOLD_CONFIRMADO: float = 0.85
THRESHOLD_REVISAO: float = 0.50


class ResultadoValidacao(BaseModel):
    processo_id: int
    parte_id: int
    polo: Literal["autor", "reu"]
    nome_tribunal: str = Field(..., max_length=300)
    nome_sistema: str | None = Field(default=None, max_length=300)
    score_ia: Decimal = Field(..., ge=Decimal("0.000"), le=Decimal("1.000"))
    status: StatusValidacao = "sem_match"
    motivo_ia: str | None = None
    revisado_por: str | None = Field(default=None, max_length=100)
    revisado_em: datetime | None = None

    @model_validator(mode="after")
    def inferir_status(self) -> "ResultadoValidacao":
        """
        Deriva o status automaticamente a partir do score_ia,
        a menos que já esteja definido como 'aprovado_manual'.
        """
        if self.status == "aprovado_manual":
            return self

        score = float(self.score_ia)
        if score >= THRESHOLD_CONFIRMADO:
            self.status = "confirmado"
        elif score >= THRESHOLD_REVISAO:
            self.status = "revisao"
        else:
            self.status = "sem_match"

        return self
