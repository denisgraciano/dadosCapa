-- V002__create_partes.sql
-- Partes processuais vinculadas a cada processo

CREATE TABLE IF NOT EXISTS partes (
    id              INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    processo_id     INT UNSIGNED    NOT NULL,
    polo            ENUM('autor', 'reu', 'advogado', 'outro') NOT NULL,
    nome_tribunal   VARCHAR(300)    NOT NULL,
    documento       VARCHAR(20)     NULL,
    criado_em       DATETIME        NOT NULL DEFAULT NOW(),

    PRIMARY KEY (id),
    UNIQUE KEY uq_partes_processo_polo_nome (processo_id, polo, nome_tribunal),
    CONSTRAINT fk_partes_processo
        FOREIGN KEY (processo_id) REFERENCES processos (id)
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
