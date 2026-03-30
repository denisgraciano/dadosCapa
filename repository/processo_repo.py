"""
repository/processo_repo.py — Persistência de processos e partes no MySQL.
"""

from datetime import date
from decimal import Decimal
from typing import Any

import mysql.connector
from loguru import logger

from models.processo import DadosCapa, Parte


class ProcessoRepository:
    """Gerencia persistência de processos e partes processuais."""

    def __init__(self, db_config: dict[str, Any]) -> None:
        self._db_config = db_config

    def _connect(self) -> mysql.connector.MySQLConnection:
        return mysql.connector.connect(**self._db_config)

    def upsert_processo(self, dados: DadosCapa) -> int:
        """
        Insere ou atualiza a capa do processo pelo CNJ.
        Retorna o processo_id gerado ou existente.
        """
        sql = """
            INSERT INTO processos
                (cnj, tribunal, classe, assunto, valor_causa, vara, juiz,
                 instancia, status, data_distribuicao, fonte, raw_json, ultima_mov)
            VALUES
                (%(cnj)s, %(tribunal)s, %(classe)s, %(assunto)s, %(valor_causa)s,
                 %(vara)s, %(juiz)s, %(instancia)s, %(status)s, %(data_distribuicao)s,
                 %(fonte)s, %(raw_json)s, %(ultima_mov)s)
            ON DUPLICATE KEY UPDATE
                tribunal         = VALUES(tribunal),
                classe           = VALUES(classe),
                assunto          = VALUES(assunto),
                valor_causa      = VALUES(valor_causa),
                vara             = VALUES(vara),
                juiz             = VALUES(juiz),
                instancia        = VALUES(instancia),
                status           = VALUES(status),
                data_distribuicao = VALUES(data_distribuicao),
                fonte            = VALUES(fonte),
                raw_json         = VALUES(raw_json),
                ultima_mov       = VALUES(ultima_mov)
        """
        import json as _json

        params = {
            "cnj": dados.cnj,
            "tribunal": dados.tribunal,
            "classe": dados.classe,
            "assunto": dados.assunto,
            "valor_causa": float(dados.valor_causa) if dados.valor_causa is not None else None,
            "vara": dados.vara,
            "juiz": dados.juiz,
            "instancia": dados.instancia,
            "status": dados.status,
            "data_distribuicao": dados.data_distribuicao,
            "fonte": dados.fonte,
            "raw_json": _json.dumps(dados.raw_json, ensure_ascii=False) if dados.raw_json else None,
            "ultima_mov": dados.ultima_mov,
        }

        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            conn.commit()

            if cursor.lastrowid:
                return cursor.lastrowid

            # ON DUPLICATE KEY UPDATE — busca o id existente
            cursor.execute("SELECT id FROM processos WHERE cnj = %s", (dados.cnj,))
            row = cursor.fetchone()
            return row[0] if row else 0
        finally:
            conn.close()

    def inserir_partes(self, processo_id: int, partes: list[Parte]) -> list[int]:
        """
        Insere partes com INSERT IGNORE (deduplicação por polo + nome_tribunal).
        Retorna lista de parte_ids (incluindo pré-existentes).
        """
        sql_insert = """
            INSERT IGNORE INTO partes (processo_id, polo, nome_tribunal, documento)
            VALUES (%s, %s, %s, %s)
        """
        sql_select = """
            SELECT id FROM partes
            WHERE processo_id = %s AND polo = %s AND nome_tribunal = %s
        """
        ids: list[int] = []

        conn = self._connect()
        try:
            cursor = conn.cursor()
            for parte in partes:
                cursor.execute(
                    sql_insert,
                    (processo_id, parte.polo, parte.nome_tribunal, parte.documento),
                )
                conn.commit()
                cursor.execute(sql_select, (processo_id, parte.polo, parte.nome_tribunal))
                row = cursor.fetchone()
                if row:
                    ids.append(row[0])
        finally:
            conn.close()

        return ids

    def atualizar_ultima_mov(self, processo_id: int, ultima_mov: date) -> None:
        """Atualiza o campo ultima_mov após processamento de movimentações."""
        sql = "UPDATE processos SET ultima_mov = %s WHERE id = %s"
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, (ultima_mov, processo_id))
            conn.commit()
        finally:
            conn.close()

    def buscar_ultima_mov(self, cnj: str) -> date | None:
        """Retorna a data da última movimentação registrada para um CNJ."""
        sql = "SELECT ultima_mov FROM processos WHERE cnj = %s"
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, (cnj,))
            row = cursor.fetchone()
            return row[0] if row and row[0] else None
        finally:
            conn.close()
