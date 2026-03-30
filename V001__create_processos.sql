-- V001__create_processos.sql
-- Tabela principal de processos judiciais

CREATE TABLE IF NOT EXISTS processos (
    id                  INT UNSIGNED      NOT NULL AUTO_INCREMENT,
    cnj                 VARCHAR(20)       NOT NULL,
    tribunal            VARCHAR(20)       NOT NULL,
    classe              VARCHAR(200)      NULL,
    assunto             VARCHAR(500)      NULL,
    valor_causa         DECIMAL(15, 2)    NULL,
    vara                VARCHAR(200)      NULL,
    juiz                VARCHAR(200)      NULL,
    instancia           ENUM('1grau', '2grau', 'stj', 'stf', 'tst') NULL,
    status              VARCHAR(100)      NULL,
    data_distribuicao   DATE              NULL,
    fonte               ENUM('datajud', 'scraping') NOT NULL,
    raw_json            JSON              NULL,
    ultima_mov          DATE              NULL,
    criado_em           DATETIME          NOT NULL DEFAULT NOW(),
    atualizado_em       DATETIME          NOT NULL DEFAULT NOW() ON UPDATE NOW(),

    PRIMARY KEY (id),
    UNIQUE KEY uq_processos_cnj (cnj)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
