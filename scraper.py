"""
collectors/scraper.py — Coletor fallback via Playwright (scraping headless).
Ativado apenas quando DatajudCollector lança CollectorUnavailableError.
Implementação de referência: TJ-SP (esaj.tjsp.jus.br).
"""

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from loguru import logger
from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeout

from collectors.base import BaseCollector, CaptchaRequiredError, CollectorUnavailableError
from models.processo import DadosCapa, Movimentacao, Parte


TJSP_URL = "https://esaj.tjsp.jus.br/cpopg/search.do"

# Seletores CSS do portal TJ-SP (podem mudar com atualizações do portal)
_SEL_CAPTCHA = "#captchaLinkCheckbox, .g-recaptcha, #recaptcha"
_SEL_CLASSE = "#classeProcesso"
_SEL_ASSUNTO = "#assuntoProcesso"
_SEL_VARA = "#varaProcesso"
_SEL_JUIZ = "#juizProcesso"
_SEL_VALOR = "#valorAcaoProcesso"
_SEL_DATA_DIST = "#dataDistribuicaoProcesso"
_SEL_STATUS = "#situacaoProcesso"
_SEL_PARTES_TABLE = "#tableTodasPartes, #tablePartesPrincipais"
_SEL_MOVIMENTACOES = "#tabelaTodasMovimentacoes tr, #tabelaUltimasMovimentacoes tr"


def _parse_date_br(value: str | None) -> date | None:
    """Converte data no formato brasileiro DD/MM/YYYY para date."""
    if not value:
        return None
    value = value.strip()
    for fmt in ("%d/%m/%Y", "%d/%m/%Y %H:%M"):
        try:
            return datetime.strptime(value[:len(fmt)], fmt).date()
        except ValueError:
            continue
    return None


def _parse_valor(value: str | None) -> Decimal | None:
    """Converte string de valor monetário brasileiro para Decimal."""
    if not value:
        return None
    # Remove R$, pontos de milhar e substitui vírgula por ponto
    cleaned = value.strip().replace("R$", "").replace(".", "").replace(",", ".").strip()
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


class ScraperCollector(BaseCollector):
    """
    Coletor via scraping headless com Playwright.
    Referência: portal TJ-SP (esaj.tjsp.jus.br).
    Outros tribunais lançam CollectorUnavailableError — extensível via _scrape_X.
    """

    TRIBUNAIS_SUPORTADOS = {"tjsp"}

    def __init__(self, tribunal: str) -> None:
        self._tribunal = tribunal.lower()

    async def collect(self, cnj: str) -> DadosCapa:
        """Coleta dados do processo via scraping, conforme tribunal configurado."""
        if self._tribunal not in self.TRIBUNAIS_SUPORTADOS:
            raise CollectorUnavailableError(
                f"ScraperCollector não suporta tribunal '{self._tribunal}'"
            )

        logger.info(f"[ScraperCollector] Iniciando scraping CNJ={cnj} tribunal={self._tribunal}")

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                )
            )
            page = await context.new_page()
            try:
                dados = await self._scrape_tjsp(page, cnj)
            finally:
                await browser.close()

        return dados

    async def _scrape_tjsp(self, page: Page, cnj: str) -> DadosCapa:
        """Scraping do portal TJ-SP."""
        try:
            await page.goto(TJSP_URL, timeout=30_000)
            await page.fill('input[name="numeroDigitoAnoUnificado"]', cnj[:15])
            await page.fill('input[name="foroNumeroUnificado"]', cnj[15:])
            await page.click('input[type="submit"], button[type="submit"]')
            await page.wait_for_load_state("networkidle", timeout=30_000)
        except PlaywrightTimeout as exc:
            raise CollectorUnavailableError(f"Timeout ao acessar TJ-SP: {exc}") from exc

        # Detecta CAPTCHA antes de tentar extrair dados
        captcha = await page.query_selector(_SEL_CAPTCHA)
        if captcha:
            logger.warning(f"[ScraperCollector] CAPTCHA detectado para CNJ={cnj}")
            raise CaptchaRequiredError(f"CAPTCHA bloqueou scraping do CNJ={cnj}")

        capa = await self._extrair_capa(page, cnj)
        partes = await self._extrair_partes(page)
        movimentacoes = await self._extrair_movimentacoes(page)

        ultima_mov: date | None = None
        if movimentacoes:
            ultima_mov = max(m.data_mov for m in movimentacoes)

        return DadosCapa(
            cnj=cnj,
            tribunal="tjsp",
            fonte="scraping",
            ultima_mov=ultima_mov,
            partes=partes,
            movimentacoes=movimentacoes,
            **capa,
        )

    async def _extrair_capa(self, page: Page, cnj: str) -> dict[str, Any]:
        """Extrai campos da capa do processo."""

        async def texto(seletor: str) -> str | None:
            el = await page.query_selector(seletor)
            return (await el.inner_text()).strip() if el else None

        return {
            "classe": await texto(_SEL_CLASSE),
            "assunto": await texto(_SEL_ASSUNTO),
            "vara": await texto(_SEL_VARA),
            "juiz": await texto(_SEL_JUIZ),
            "valor_causa": _parse_valor(await texto(_SEL_VALOR)),
            "data_distribuicao": _parse_date_br(await texto(_SEL_DATA_DIST)),
            "status": await texto(_SEL_STATUS),
            "instancia": "1grau",  # TJ-SP é sempre 1º grau neste endpoint
            "raw_json": None,
        }

    async def _extrair_partes(self, page: Page) -> list[Parte]:
        """Extrai partes processuais da tabela do portal TJ-SP."""
        partes: list[Parte] = []
        polo_map = {
            "autor": "autor",
            "autora": "autor",
            "réu": "reu",
            "ré": "reu",
            "advogado": "advogado",
            "advogada": "advogado",
        }

        rows = await page.query_selector_all(f"{_SEL_PARTES_TABLE} tr")
        for row in rows:
            cells = await row.query_selector_all("td")
            if len(cells) < 2:
                continue

            polo_text = (await cells[0].inner_text()).strip().lower().rstrip(":")
            nome = (await cells[1].inner_text()).strip().split("\n")[0].strip()

            if not nome:
                continue

            polo = polo_map.get(polo_text, "outro")
            partes.append(Parte(polo=polo, nome_tribunal=nome))

        return partes

    async def _extrair_movimentacoes(self, page: Page) -> list[Movimentacao]:
        """Extrai movimentações da tabela do portal TJ-SP."""
        movimentacoes: list[Movimentacao] = []

        rows = await page.query_selector_all(_SEL_MOVIMENTACOES)
        for row in rows:
            cells = await row.query_selector_all("td")
            if len(cells) < 3:
                continue

            data_text = (await cells[0].inner_text()).strip()
            descricao = (await cells[2].inner_text()).strip()

            data_mov = _parse_date_br(data_text)
            if data_mov is None or not descricao:
                continue

            movimentacoes.append(
                Movimentacao(
                    data_mov=data_mov,
                    descricao=descricao,
                )
            )

        return movimentacoes
