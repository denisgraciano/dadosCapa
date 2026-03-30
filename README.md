# process-monitor

Monitoramento Automatizado de Processos Judiciais Brasileiros

> Versão 1.0 · Python 3.12 · 2025 · Uso Interno

## Instalação

```bash
# 1. Clone e entre na pasta
git clone <repo-url> && cd process-monitor

# 2. Crie e ative o virtualenv
python -m venv .venv && source .venv/bin/activate

# 3. Instale as dependências
pip install -r requirements.txt

# 4. Instale os browsers do Playwright
playwright install chromium

# 5. Configure o ambiente
cp .env.example .env && nano .env
```

## Migrations

```bash
mysql -u root -p process_monitor < db/migrations/V001__create_processos.sql
mysql -u root -p process_monitor < db/migrations/V002__create_partes.sql
mysql -u root -p process_monitor < db/migrations/V003__create_movimentacoes.sql
mysql -u root -p process_monitor < db/migrations/V004__create_validacoes_partes.sql
```

## Uso

```bash
# Sincroniza todos os CNJs definidos em CNJS no .env
python main.py sync

# Sincroniza um único CNJ específico
python main.py sync --cnj 10022541820258110018
```

## Thresholds de Validação IA

| Score | Status | Ação |
|-------|--------|------|
| ≥ 0.85 | `confirmado` | Gravado automaticamente |
| 0.50 – 0.84 | `revisao` | Fila de revisão humana |
| < 0.50 | `sem_match` | Alerta no log |

Para ajustar os thresholds: edite `models/validacao.py` → `THRESHOLD_CONFIRMADO` e `THRESHOLD_REVISAO`.
