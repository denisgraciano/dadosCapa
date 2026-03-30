"""
ai/validador_partes.py — Valida correspondência de partes processuais usando Claude API.
Compara o nome extraído do tribunal com a lista de partes do sistema interno.
"""

import json
from decimal import Decimal
from typing import Any

import anthropic
from loguru import logger

from config import settings

_MODEL = "claude-sonnet-4-20250514"
_MAX_TOKENS = 300

_PROMPT_TEMPLATE = """Você é um sistema de validação de partes processuais.

Compare o nome extraído do tribunal com a lista de partes do sistema interno e determine
se há correspondência, mesmo que os nomes não sejam idênticos (abreviações, variações,
nome fantasia vs. razão social, etc.).

Nome extraído do tribunal: "{nome_tribunal}"

Partes do sistema interno:
{partes_json}

Responda APENAS com um objeto JSON válido, sem texto adicional, sem markdown, sem explicações:
{{
  "match_encontrado": <true ou false>,
  "indice_match": <índice (0-based) da parte correspondente, ou null se não encontrado>,
  "score": <número decimal entre 0.0 e 1.0 indicando confiança da correspondência>,
  "motivo": "<explicação breve em português da decisão>"
}}"""


class ValidadorPartes:
    """Validador de partes processuais usando Claude Sonnet."""

    def __init__(self) -> None:
        self._client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    def validar(
        self,
        nome_tribunal: str,
        partes_sistema: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Valida se nome_tribunal corresponde a alguma parte do sistema interno.

        Args:
            nome_tribunal: Nome da parte conforme registrado no tribunal.
            partes_sistema: Lista de partes do sistema interno com nome, documento e polo.

        Returns:
            Dicionário com match_encontrado, indice_match, score e motivo.
            Em caso de erro de parse, retorna score=0.0 e match_encontrado=False.
        """
        prompt = _PROMPT_TEMPLATE.format(
            nome_tribunal=nome_tribunal,
            partes_json=json.dumps(partes_sistema, ensure_ascii=False, indent=2),
        )

        try:
            message = self._client.messages.create(
                model=_MODEL,
                max_tokens=_MAX_TOKENS,
                messages=[{"role": "user", "content": prompt}],
            )
            raw_text = message.content[0].text.strip()
            return self._parse_resposta(raw_text, nome_tribunal)

        except anthropic.APIError as exc:
            logger.error(f"[ValidadorPartes] Erro na API Claude para '{nome_tribunal}': {exc}")
            return self._fallback_erro()

    def _parse_resposta(self, raw_text: str, nome_tribunal: str) -> dict[str, Any]:
        """
        Faz parse seguro do JSON retornado pela IA.
        Em caso de resposta malformada, registra warning e retorna fallback seguro.
        """
        # Remove possíveis blocos de código que o modelo possa ter inserido
        text = raw_text.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

        try:
            data = json.loads(text)
            # Garante que score é float válido entre 0 e 1
            score = float(data.get("score", 0.0))
            score = max(0.0, min(1.0, score))
            return {
                "match_encontrado": bool(data.get("match_encontrado", False)),
                "indice_match": data.get("indice_match"),
                "score": Decimal(str(round(score, 3))),
                "motivo": str(data.get("motivo", "")),
            }
        except (json.JSONDecodeError, ValueError, KeyError) as exc:
            logger.warning(
                f"[ValidadorPartes] JSON inválido para '{nome_tribunal}': {exc} | raw={raw_text!r}"
            )
            return self._fallback_erro()

    @staticmethod
    def _fallback_erro() -> dict[str, Any]:
        """Retorna resultado seguro em caso de erro — sem interromper o pipeline."""
        return {
            "match_encontrado": False,
            "indice_match": None,
            "score": Decimal("0.000"),
            "motivo": "Erro ao processar resposta da IA",
        }
