"""
repository/validacao_repo.py — Persistência dos resultados de validação de partes via IA.
"""

from typing import Any

import mysql.connector

from models.validacao import ResultadoValidacao


class ValidacaoRepository:
    """Gerencia persistência dos resultados de validação de partes."""

    def __init__(self, db_config: dict[str, Any]) -> None:
        self._db_config = db_config

    def _connect(self) -> mysql.connector.MySQLConnection:
        return mysql.connector.connect(**self._db_config)

    def inserir(self, resultado: ResultadoValidacao) -> None:
        """Grava o resultado da validação de uma parte em validacoes_partes."""
        sql = """
            INSERT INTO validacoes_partes
                (processo_id, parte_id, polo, nome_tribunal, nome_sistema,
                 score_ia, status, motivo_ia, revisado_por, revisado_em)
            VALUES
                (%(processo_id)s, %(parte_id)s, %(polo)s, %(nome_tribunal)s, %(nome_sistema)s,
                 %(score_ia)s, %(status)s, %(motivo_ia)s, %(revisado_por)s, %(revisado_em)s)
        """
        params = {
            "processo_id": resultado.processo_id,
            "parte_id": resultado.parte_id,
            "polo": resultado.polo,
            "nome_tribunal": resultado.nome_tribunal,
            "nome_sistema": resultado.nome_sistema,
            "score_ia": float(resultado.score_ia),
            "status": resultado.status,
            "motivo_ia": resultado.motivo_ia,
            "revisado_por": resultado.revisado_por,
            "revisado_em": resultado.revisado_em,
        }
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            conn.commit()
        finally:
            conn.close()
