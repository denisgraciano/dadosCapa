"""
collectors/escavador.py — Coletor fallback via scraping do site Escavador.

Ativado quando o scraper do tribunal é bloqueado por CAPTCHA.
Não requer autenticação — usa a página pública de busca.

URL de busca: https://www.escavador.com/busca?q={cnj}
URL do processo: https://www.escavador.com/processos/{id}/...
"""

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from loguru import logger
from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeout

from collectors.base import BaseCollector, CollectorUnavailableError
from models.processo import DadosCapa, Movimentacao, Parte


ESCAVADOR_BUSCA_URL = "https://www.escavador.com/busca"
ESCAVADOR_BASE_URL  = "https://www.escavador.com"

# Seletores da página de resultados de busca
_SEL_RESULTADO_PROCESSO = "a[href*='/processos/']"
_SEL_SEM_RESULTADO      = ".sem-resultado, .empty-state, [class*='empty'], [class*='nao-encontrado']"

# Seletores da página do processo
_SEL_POLO_ATIVO   = (
    "[class*='polo-ativo'] [class*='nome'], "
    "[class*='parte-ativa'] [class*='nome'], "
    "dt:has-text('Polo ativo') + dd, "
    "dt:has-text('Autor') + dd, "
    "h3:has-text('Polo ativo') ~ * [class*='nome']"
)
_SEL_POLO_PASSIVO = (
    "[class*='polo-passivo'] [class*='nome'], "
    "[class*='parte-passiva'] [class*='nome'], "
    "dt:has-text('Polo passivo') + dd, "
    "dt:has-text('Réu') + dd, "
    "h3:has-text('Polo passivo') ~ * [class*='nome']"
)
_SEL_CLASSE    = "[class*='classe'], dt:has-text('Classe') + dd"
_SEL_ASSUNTO   = "[class*='assunto'], dt:has-text('Assunto') + dd"
_SEL_VARA      = "[class*='orgao'], [class*='vara'], dt:has-text('Vara') + dd, dt:has-text('Órgão') + dd"
_SEL_DATA_INI  = "dt:has-text('Data de início') + dd, dt:has-text('Distribuição') + dd, [class*='data-inicio']"
_SEL_MOVS      = "[class*='movimentacao'], [class*='andamento'], .timeline-item"


def _parse_date_br(value: str | None) -> date | None:
    if not value:
        return None
    value = value.strip().split(" ")[0]
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _parse_decimal(value: str | None) -> Decimal | None:
    if not value:
        return None
    cleaned = re.sub(r"[^\d,]", "", value).replace(",", ".")
    try:
        return Decimal(cleaned) if cleaned else None
    except InvalidOperation:
        return None


class EscavadorCollector(BaseCollector):
    """
    Coletor via scraping público do Escavador.
    Acessa a busca pública sem necessidade de login ou API key.
    Usado como terceiro fallback quando o scraper do tribunal falha por CAPTCHA.
    """

    async def collect(self, cnj: str) -> DadosCapa:
        """Busca o processo no Escavador e extrai dados públicos disponíveis."""
        logger.info(f"[EscavadorCollector] Buscando CNJ={cnj}")

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                ),
                locale="pt-BR",
            )
            page = await context.new_page()
            try:
                dados = await self._buscar_e_extrair(page, cnj)
            finally:
                await browser.close()

        return dados

    async def _buscar_e_extrair(self, page: Page, cnj: str) -> DadosCapa:
        """Navega pelo Escavador: busca → clica no resultado → extrai dados."""

        # ── Etapa 1: Página de busca ─────────────────────────────────────────
        cnj_fmt = self._formatar_cnj(cnj)
        url_busca = f"{ESCAVADOR_BUSCA_URL}?q={cnj_fmt}"
        logger.debug(f"[EscavadorCollector] Buscando em: {url_busca}")

        try:
            await page.goto(url_busca, timeout=60_000, wait_until="domcontentloaded")
            await page.wait_for_load_state("networkidle", timeout=20_000)
        except PlaywrightTimeout as exc:
            raise CollectorUnavailableError(
                f"Timeout ao acessar Escavador: {exc}"
            ) from exc

        titulo = await page.title()
        logger.debug(f"[EscavadorCollector] Página busca: '{titulo}'")

        # ── Etapa 2: Encontra link do processo nos resultados ────────────────
        link_processo = await self._encontrar_link_processo(page, cnj)

        if not link_processo:
            raise CollectorUnavailableError(
                f"Processo CNJ={cnj} não encontrado no Escavador"
            )

        # ── Etapa 3: Abre página do processo ─────────────────────────────────
        url_processo = (
            link_processo
            if link_processo.startswith("http")
            else f"{ESCAVADOR_BASE_URL}{link_processo}"
        )
        logger.debug(f"[EscavadorCollector] Abrindo processo: {url_processo}")

        try:
            await page.goto(url_processo, timeout=60_000, wait_until="domcontentloaded")
            await page.wait_for_load_state("networkidle", timeout=20_000)
        except PlaywrightTimeout as exc:
            raise CollectorUnavailableError(
                f"Timeout ao abrir processo no Escavador: {exc}"
            ) from exc

        logger.debug(f"[EscavadorCollector] URL final: {page.url}")

        # ── Etapa 4: Extração ─────────────────────────────────────────────────
        capa   = await self._extrair_capa(page)
        partes = await self._extrair_partes(page)
        movs   = await self._extrair_movimentacoes(page)

        logger.info(
            f"[EscavadorCollector] Extraído: classe={capa.get('classe')!r} "
            f"partes={len(partes)} movimentações={len(movs)}"
        )

        ultima_mov = max((m.data_mov for m in movs), default=None)

        return DadosCapa(
            cnj=cnj,
            tribunal=self._extrair_tribunal_da_url(url_processo),
            fonte="scraping",
            ultima_mov=ultima_mov,
            partes=partes,
            movimentacoes=movs,
            **capa,
        )

    async def _encontrar_link_processo(self, page: Page, cnj: str) -> str | None:
        """
        Procura o link do processo na página de resultados do Escavador.
        Estratégia 1: link com /processos/ que contenha o CNJ
        Estratégia 2: primeiro link com /processos/ na página
        """
        cnj_digits = re.sub(r"\D", "", cnj)

        # Todos os links de processo na página
        links = await page.query_selector_all(_SEL_RESULTADO_PROCESSO)
        logger.debug(f"[EscavadorCollector] Links de processo encontrados: {len(links)}")

        for link_el in links:
            href = await link_el.get_attribute("href") or ""
            texto = (await link_el.inner_text()).strip()

            # Verifica se o link ou texto contém dígitos do CNJ
            href_digits = re.sub(r"\D", "", href)
            texto_digits = re.sub(r"\D", "", texto)

            if cnj_digits in href_digits or cnj_digits in texto_digits:
                logger.debug(f"[EscavadorCollector] Link match por CNJ: {href!r}")
                return href

        # Fallback: primeiro link de processo (pode ser resultado da busca)
        if links:
            href = await links[0].get_attribute("href") or ""
            logger.debug(f"[EscavadorCollector] Link fallback (primeiro resultado): {href!r}")
            return href

        return None

    def _formatar_cnj(self, cnj: str) -> str:
        """Formata CNJ para URL: NNNNNNN-DD.AAAA.J.TT.FFFF"""
        d = re.sub(r"\D", "", cnj)
        if len(d) != 20:
            return cnj
        return f"{d[0:7]}-{d[7:9]}.{d[9:13]}.{d[13]}.{d[14:16]}.{d[16:20]}"

    def _extrair_tribunal_da_url(self, url: str) -> str:
        """Tenta extrair a sigla do tribunal a partir da URL do processo."""
        url_lower = url.lower()
        tribunais = [
            "tjsp", "tjrj", "tjmg", "tjrs", "tjpr", "tjba", "tjsc",
            "tjce", "tjpe", "tjgo", "tjma", "tjms", "tjmt", "tjal",
            "tjes", "tjrn", "tjpi", "tjse", "tjam", "tjpa", "trf1",
            "trf2", "trf3", "trf4", "trf5", "stj", "stf", "tst",
        ]
        for t in tribunais:
            if t in url_lower:
                return t
        return "desconhecido"

    async def _extrair_capa(self, page: Page) -> dict[str, Any]:
        """Extrai campos da capa do processo na página do Escavador."""

        async def texto_sel(seletor: str) -> str | None:
            el = await page.query_selector(seletor)
            if not el:
                return None
            t = (await el.inner_text()).strip()
            return t or None

        async def buscar_por_label(*labels: str) -> str | None:
            for label in labels:
                el = await page.query_selector(
                    f"dt:has-text('{label}') + dd, "
                    f"th:has-text('{label}') + td, "
                    f"span:has-text('{label}') + span"
                )
                if el:
                    t = (await el.inner_text()).strip()
                    if t:
                        return t
            return None

        classe  = await buscar_por_label("Classe", "Tipo")
        assunto = await buscar_por_label("Assunto", "Matéria")
        vara    = await buscar_por_label("Vara", "Órgão julgador", "Órgão", "Juízo")
        juiz    = await buscar_por_label("Magistrado", "Juiz", "Juíza", "Relator")
        data    = await buscar_por_label("Data de início", "Distribuição", "Data")
        status  = await buscar_por_label("Situação", "Status", "Fase")
        valor   = await buscar_por_label("Valor da causa", "Valor da ação")

        # Fallback genérico — tenta seletores diretos
        if not classe:
            classe = await texto_sel(_SEL_CLASSE)
        if not vara:
            vara = await texto_sel(_SEL_VARA)

        logger.debug(
            f"[EscavadorCollector] capa: classe={classe!r} vara={vara!r} "
            f"juiz={juiz!r} data={data!r}"
        )

        return {
            "classe": classe,
            "assunto": assunto,
            "vara": vara,
            "juiz": juiz,
            "valor_causa": _parse_decimal(valor),
            "data_distribuicao": _parse_date_br(data),
            "status": status,
            "instancia": None,
            "raw_json": None,
        }

    async def _extrair_partes(self, page: Page) -> list[Parte]:
        """
        Extrai partes do processo na página do Escavador.
        O Escavador separa polo ativo e passivo em seções distintas.
        Estratégia 1: seções específicas de polo
        Estratégia 2: tabela/lista de envolvidos com rótulo de polo
        """
        partes: list[Parte] = []

        # Estratégia 1: seções de polo ativo e passivo
        for polo, seletores in [
            ("autor", [
                "[class*='polo-ativo'] [class*='nome']",
                "[class*='polo-ativo'] a",
                "section:has(h3:has-text('Polo ativo')) [class*='nome']",
                "section:has(h3:has-text('Polo ativo')) a[href*='/sobre/']",
                "h3:has-text('Polo ativo') ~ div a[href*='/sobre/']",
                "h3:has-text('Polo ativo') ~ ul li",
            ]),
            ("reu", [
                "[class*='polo-passivo'] [class*='nome']",
                "[class*='polo-passivo'] a",
                "section:has(h3:has-text('Polo passivo')) [class*='nome']",
                "section:has(h3:has-text('Polo passivo')) a[href*='/sobre/']",
                "h3:has-text('Polo passivo') ~ div a[href*='/sobre/']",
                "h3:has-text('Polo passivo') ~ ul li",
            ]),
        ]:
            for seletor in seletores:
                elementos = await page.query_selector_all(seletor)
                if elementos:
                    for el in elementos:
                        nome = (await el.inner_text()).strip().split("\n")[0].strip()
                        nome = re.sub(r"\s+", " ", nome)
                        if nome and len(nome) > 2:
                            partes.append(Parte(polo=polo, nome_tribunal=nome))
                            logger.debug(
                                f"[EscavadorCollector] Parte: polo={polo!r} nome={nome!r} "
                                f"(seletor={seletor!r})"
                            )
                    break  # Encontrou com este seletor, para

        # Estratégia 2 — fallback por rótulo textual em lista genérica
        if not partes:
            await self._extrair_partes_fallback(page, partes)

        logger.debug(f"[EscavadorCollector] Total partes extraídas: {len(partes)}")
        return partes

    async def _extrair_partes_fallback(self, page: Page, partes: list[Parte]) -> None:
        """Fallback: procura labels 'Polo ativo'/'Polo passivo' no texto da página."""
        polo_map = {
            "polo ativo": "autor",
            "autor": "autor",
            "requerente": "autor",
            "polo passivo": "reu",
            "réu": "reu",
            "requerido": "reu",
        }

        # Tenta encontrar itens de lista com rótulo de polo
        items = await page.query_selector_all("li, tr, [class*='envolvido'], [class*='parte']")
        polo_atual: str = "outro"

        for item in items:
            texto = (await item.inner_text()).strip().lower()
            # Verifica se é um cabeçalho de polo
            for label, polo in polo_map.items():
                if label in texto and len(texto) < 50:
                    polo_atual = polo
                    break
            else:
                # É um item de parte
                nome_raw = (await item.inner_text()).strip().split("\n")[0].strip()
                nome = re.sub(r"\s+", " ", nome_raw)
                if nome and len(nome) > 3 and polo_atual != "outro":
                    partes.append(Parte(polo=polo_atual, nome_tribunal=nome))

    async def _extrair_movimentacoes(self, page: Page) -> list[Movimentacao]:
        """Extrai movimentações/andamentos listados na página do Escavador."""
        movimentacoes: list[Movimentacao] = []

        # Tenta expandir "Ver todas as movimentações" se existir
        for seletor in [
            "button:has-text('Ver todas')",
            "a:has-text('Ver todas')",
            "button:has-text('Mais movimentações')",
            "[class*='carregar-mais']",
        ]:
            botao = await page.query_selector(seletor)
            if botao:
                try:
                    await botao.click()
                    await page.wait_for_load_state("networkidle", timeout=10_000)
                except Exception:
                    pass
                break

        items = await page.query_selector_all(_SEL_MOVS)
        logger.debug(f"[EscavadorCollector] Movimentações encontradas: {len(items)}")

        for item in items:
            texto_completo = (await item.inner_text()).strip()
            linhas = [l.strip() for l in texto_completo.split("\n") if l.strip()]
            if not linhas:
                continue

            # Tenta encontrar data no primeiro elemento com formato DD/MM/YYYY
            data_mov: date | None = None
            descricao_linhas: list[str] = []

            for linha in linhas:
                if data_mov is None:
                    d = _parse_date_br(linha)
                    if d:
                        data_mov = d
                        continue
                descricao_linhas.append(linha)

            descricao = " ".join(descricao_linhas).strip()

            if data_mov and descricao:
                movimentacoes.append(
                    Movimentacao(data_mov=data_mov, descricao=descricao)
                )

        return movimentacoes