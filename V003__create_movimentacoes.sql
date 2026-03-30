-- V003__create_movimentacoes.sql
-- Movimentações processuais com deduplicação por SHA-256

CREATE TABLE IF NOT EXISTS movimentacoes (
    id              INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    processo_id     INT UNSIGNED    NOT NULL,
    data_mov        DATE            NOT NULL,
    codigo_mov      VARCHAR(20)     NULL,
    descricao       TEXT            NOT NULL,
    complemento     TEXT            NULL,
    hash_conteudo   CHAR(64)        NOT NULL,
    criado_em       DATETIME        NOT NULL DEFAULT NOW(),

    PRIMARY KEY (id),
    UNIQUE KEY uq_movimentacoes_hash (hash_conteudo),
    KEY idx_movimentacoes_processo (processo_id),
    KEY idx_movimentacoes_data (data_mov),
    CONSTRAINT fk_movimentacoes_processo
        FOREIGN KEY (processo_id) REFERENCES processos (id)
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
