"""
repository/movimentacao_repo.py — Persistência de movimentações processuais com deduplicação.
"""

from typing import Any

import mysql.connector
from loguru import logger

from models.processo import Movimentacao
from pipeline.hash_util import sha256_movimentacao


class MovimentacaoRepository:
    """Gerencia persistência de movimentações com deduplicação por SHA-256."""

    def __init__(self, db_config: dict[str, Any]) -> None:
        self._db_config = db_config

    def _connect(self) -> mysql.connector.MySQLConnection:
        return mysql.connector.connect(**self._db_config)

    def inserir_novas(self, processo_id: int, movimentacoes: list[Movimentacao]) -> int:
        """
        Insere movimentações novas, ignorando duplicatas via UNIQUE constraint em hash_conteudo.

        Returns:
            Quantidade de movimentações efetivamente inseridas.
        """
        sql = """
            INSERT IGNORE INTO movimentacoes
                (processo_id, data_mov, codigo_mov, descricao, complemento, hash_conteudo)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        inseridas = 0
        conn = self._connect()
        try:
            cursor = conn.cursor()
            for mov in movimentacoes:
                hash_val = sha256_movimentacao(mov)
                cursor.execute(
                    sql,
                    (
                        processo_id,
                        mov.data_mov,
                        mov.codigo_mov,
                        mov.descricao,
                        mov.complemento,
                        hash_val,
                    ),
                )
                if cursor.rowcount > 0:
                    inseridas += 1
            conn.commit()
        finally:
            conn.close()

        return inseridas
