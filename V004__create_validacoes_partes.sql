-- V004__create_validacoes_partes.sql
-- Resultados de validação de partes gerados pela IA (Claude Sonnet)

CREATE TABLE IF NOT EXISTS validacoes_partes (
    id              INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    processo_id     INT UNSIGNED    NOT NULL,
    parte_id        INT UNSIGNED    NOT NULL,
    polo            ENUM('autor', 'reu') NOT NULL,
    nome_tribunal   VARCHAR(300)    NOT NULL,
    nome_sistema    VARCHAR(300)    NULL,
    score_ia        DECIMAL(4, 3)   NOT NULL,
    status          ENUM('confirmado', 'revisao', 'sem_match', 'aprovado_manual')
                                    NOT NULL DEFAULT 'sem_match',
    motivo_ia       TEXT            NULL,
    revisado_por    VARCHAR(100)    NULL,
    revisado_em     DATETIME        NULL,
    criado_em       DATETIME        NOT NULL DEFAULT NOW(),

    PRIMARY KEY (id),
    KEY idx_validacoes_processo (processo_id),
    KEY idx_validacoes_status (status),
    CONSTRAINT fk_validacoes_processo
        FOREIGN KEY (processo_id) REFERENCES processos (id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_validacoes_parte
        FOREIGN KEY (parte_id) REFERENCES partes (id)
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
