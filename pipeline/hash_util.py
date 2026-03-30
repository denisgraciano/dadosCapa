"""
pipeline/hash_util.py — Geração de hash SHA-256 para deduplicação de movimentações.
"""

import hashlib

from models.processo import Movimentacao


def sha256_movimentacao(mov: Movimentacao) -> str:
    """
    Gera SHA-256 determinístico para uma movimentação.
    Campos utilizados: data_mov + codigo_mov + descricao + complemento.
    Garante idempotência: a mesma movimentação sempre gera o mesmo hash.
    """
    conteudo = "|".join([
        str(mov.data_mov),
        mov.codigo_mov or "",
        mov.descricao,
        mov.complemento or "",
    ])
    return hashlib.sha256(conteudo.encode("utf-8")).hexdigest()
