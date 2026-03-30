"""
collectors/scraper.py — Coletor fallback via Playwright (scraping headless).
Ativado apenas quando DatajudCollector lança CollectorUnavailableError.
Implementação de referência: TJ-SP (esaj.tjsp.jus.br/cpopg).
"""

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from loguru import logger
from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeout

from collectors.base import BaseCollector, CaptchaRequiredError, CollectorUnavailableError
from models.processo import DadosCapa, Movimentacao, Parte


# URL de consulta pública do ESAJ TJ-SP
TJSP_SEARCH_URL = "https://esaj.tjsp.jus.br/cpopg/search.do"

# Seletores CSS do ESAJ TJ-SP (validados contra o HTML real do portal)
_SEL_CAPTCHA    = "#captchaLinkCheckbox, .g-recaptcha, #recaptcha"
_SEL_CLASSE     = "span#classeProcesso"
_SEL_ASSUNTO    = "span#assuntoProcesso"
_SEL_VARA       = "span#varaProcesso"
_SEL_JUIZ       = "span#juizProcesso"
_SEL_VALOR      = "span#valorAcaoProcesso"
_SEL_DATA_DIST  = "span#dataDistribuicaoProcesso"
_SEL_STATUS     = "span#situacaoProcesso"

# Partes: o ESAJ exibe em table#tableTodasPartes com td.nomeParteEAdvogado
_SEL_PARTES_TABLE = "table#tableTodasPartes, table#tablePartesPrincipais"
_SEL_PARTE_NOME   = "td.nomeParteEAdvogado"
_SEL_PARTE_LABEL  = "td.label"

# Movimentações
_SEL_MOV_TABLE = "table#tabelaTodasMovimentacoes, table#tabelaUltimasMovimentacoes"

# Polo — palavras-chave que identificam o papel da parte
_POLO_MAP: dict[str, str] = {
    # ── Polo ativo / autor ────────────────────────────────────────────────
    "autor":         "autor",
    "autora":        "autor",
    "requerente":    "autor",
    "reqte":         "autor",
    "exequente":     "autor",
    "exeqte":        "autor",    # abreviação ESAJ ← encontrado em produção
    "exeqüente":     "autor",    # grafia antiga com trema
    "impetrante":    "autor",
    "imptte":        "autor",
    "apelante":      "autor",
    "aplnte":        "autor",
    "embargante":    "autor",
    "embgte":        "autor",
    "reclamante":    "autor",
    "recorrente":    "autor",
    "agravante":     "autor",
    "agravnte":      "autor",
    "paciente":      "autor",

    # ── Polo passivo / réu ────────────────────────────────────────────────
    "réu":           "reu",
    "reu":           "reu",
    "ré":            "reu",
    "requerido":     "reu",
    "requerida":     "reu",
    "reqdo":         "reu",
    "reqda":         "reu",
    "executado":     "reu",
    "executada":     "reu",
    "exectdo":       "reu",      # abreviação ESAJ ← encontrado em produção
    "exectda":       "reu",      # abreviação ESAJ
    "executdo":      "reu",
    "executda":      "reu",
    "impetrado":     "reu",
    "impdo":         "reu",
    "apelado":       "reu",
    "apelada":       "reu",
    "apldo":         "reu",
    "aplda":         "reu",
    "embargado":     "reu",
    "embgdo":        "reu",
    "reclamado":     "reu",
    "recorrido":     "reu",
    "agravado":      "reu",
    "coator":        "reu",

    # ── Advogados ─────────────────────────────────────────────────────────
    "advogado":      "advogado",
    "advogada":      "advogado",
    "adv":           "advogado",
    "defensor":      "advogado",
    "defensora":     "advogado",
}


def _formatar_cnj_tjsp(cnj: str) -> tuple[str, str]:
    """
    Divide o CNJ nos dois campos do formulário ESAJ:
      numeroDigitoAnoUnificado  → primeiros 15 dígitos  ex: 0034925-95.2019
      foroNumeroUnificado       → últimos 4 dígitos      ex: 8260
    O ESAJ aceita tanto com quanto sem formatação, mas usamos só dígitos.
    """
    digits = re.sub(r"\D", "", cnj)
    # CNJ: NNNNNNN DD AAAA J TT FFFF  (20 dígitos)
    # ESAJ campo 1: 7+2+4 = 13 dígitos | campo 2: últimos 4
    parte1 = digits[:13]  # nnnnnnn-dd.aaaa
    parte2 = digits[16:]  # ffff
    return parte1, parte2


def _parse_date_br(value: str | None) -> date | None:
    """Converte data BR (DD/MM/YYYY ou DD/MM/YYYY HH:MM) para date."""
    if not value:
        return None
    value = value.strip().split(" ")[0]  # descarta hora
    try:
        return datetime.strptime(value, "%d/%m/%Y").date()
    except ValueError:
        return None


def _parse_valor(value: str | None) -> Decimal | None:
    """Converte valor monetário BR (R$ 1.234,56) para Decimal."""
    if not value:
        return None
    cleaned = re.sub(r"[^\d,]", "", value).replace(",", ".")
    try:
        return Decimal(cleaned) if cleaned else None
    except InvalidOperation:
        return None



# ── TJ-RJ ─────────────────────────────────────────────────────────────────────
# Portal PROJUDI — consulta pública sem login
# URL: https://www3.tjrj.jus.br/projudi/processo/consultaPublica.do
TJRJ_SEARCH_URL = "https://www3.tjrj.jus.br/projudi/processo/consultaPublica.do"

# Seletores PROJUDI TJ-RJ
_TJRJ_SEL_CAPTCHA    = "#captcha, .g-recaptcha"
_TJRJ_SEL_CLASSE     = "span.classeProcesso, td.classeProcesso, #classeProcesso"
_TJRJ_SEL_ASSUNTO    = "span.assuntoProcesso, td.assuntoProcesso, #assuntoProcesso"
_TJRJ_SEL_VARA       = "span.orgaoJulgador, td.orgaoJulgador, #orgaoJulgador"
_TJRJ_SEL_JUIZ       = "span.magistrado, td.magistrado, #magistrado"
_TJRJ_SEL_VALOR      = "span.valorAcao, td.valorAcao, #valorAcao"
_TJRJ_SEL_DATA_DIST  = "span.dataDistribuicao, td.dataDistribuicao, #dataDistribuicao"
_TJRJ_SEL_STATUS     = "span.situacao, td.situacao, #situacao"
_TJRJ_SEL_PARTES     = "table.partes, table#partes, .partes-processo"
_TJRJ_SEL_MOVS       = "table.movimentacoes, table#movimentacoes, .movimentos-processo"

# Polo map para o PROJUDI TJ-RJ
_TJRJ_POLO_MAP: dict[str, str] = {
    "autor":        "autor",
    "autora":       "autor",
    "requerente":   "autor",
    "exequente":    "autor",
    "impetrante":   "autor",
    "apelante":     "autor",
    "embargante":   "autor",
    "reclamante":   "autor",
    "recorrente":   "autor",
    "agravante":    "autor",
    "réu":          "reu",
    "reu":          "reu",
    "ré":           "reu",
    "requerido":    "reu",
    "requerida":    "reu",
    "executado":    "reu",
    "executada":    "reu",
    "impetrado":    "reu",
    "apelado":      "reu",
    "apelada":      "reu",
    "embargado":    "reu",
    "reclamado":    "reu",
    "recorrido":    "reu",
    "agravado":     "reu",
    "advogado":     "advogado",
    "advogada":     "advogado",
    "defensor":     "advogado",
    "defensora":    "advogado",
}


def _formatar_cnj_tjrj(cnj: str) -> str:
    """
    Formata o CNJ para o campo de busca do PROJUDI TJ-RJ.
    O PROJUDI aceita o número com ou sem formatação.
    Retorna no formato: NNNNNNN-DD.AAAA.8.19.FFFF
    """
    d = re.sub(r"\D", "", cnj)
    if len(d) != 20:
        return cnj
    # NNNNNNN-DD.AAAA.J.TT.FFFF
    return f"{d[0:7]}-{d[7:9]}.{d[9:13]}.{d[13]}.{d[14:16]}.{d[16:20]}"


class ScraperCollector(BaseCollector):
    """
    Coletor via scraping headless com Playwright.
    Referência: portal TJ-SP (esaj.tjsp.jus.br/cpopg).
    Outros tribunais lançam CollectorUnavailableError — extensível via _scrape_X.
    """

    TRIBUNAIS_SUPORTADOS = {"tjsp", "tjrj"}

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
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                )
            )
            page = await context.new_page()
            try:
                if self._tribunal == "tjsp":
                    dados = await self._scrape_tjsp(page, cnj)
                elif self._tribunal == "tjrj":
                    dados = await self._scrape_tjrj(page, cnj)
                else:
                    raise CollectorUnavailableError(
                        f"ScraperCollector não tem implementação para '{self._tribunal}'"
                    )
            finally:
                await browser.close()

        return dados

    async def _scrape_tjsp(self, page: Page, cnj: str) -> DadosCapa:
        """Scraping do portal ESAJ TJ-SP."""
        parte1, parte2 = _formatar_cnj_tjsp(cnj)

        # Monta URL direta com parâmetros — evita depender do formulário
        url = (
            f"{TJSP_SEARCH_URL}"
            f"?conversationId="
            f"&cbPesquisa=NUMPROC"
            f"&numeroDigitoAnoUnificado={parte1}"
            f"&foroNumeroUnificado={parte2}"
            f"&dePesquisaNuUnificado={cnj}"
            f"&dePesquisaNuUnificado=UNIFICADO"
            f"&uuidCaptcha="
            f"&pbEnviar=Pesquisar"
        )

        logger.debug(f"[ScraperCollector] URL: {url}")

        try:
            await page.goto(url, timeout=60_000, wait_until="domcontentloaded")
            await page.wait_for_load_state("networkidle", timeout=30_000)
        except PlaywrightTimeout as exc:
            raise CollectorUnavailableError(f"Timeout ao acessar TJ-SP: {exc}") from exc

        # Detecta CAPTCHA
        captcha = await page.query_selector(_SEL_CAPTCHA)
        if captcha:
            logger.warning(f"[ScraperCollector] CAPTCHA detectado para CNJ={cnj}")
            raise CaptchaRequiredError(f"CAPTCHA bloqueou scraping do CNJ={cnj}")

        # Log diagnóstico — ajuda a depurar se a página carregou certo
        titulo = await page.title()
        logger.debug(f"[ScraperCollector] Página carregada: '{titulo}' | URL final: {page.url}")

        capa = await self._extrair_capa(page)
        partes = await self._extrair_partes(page)
        movimentacoes = await self._extrair_movimentacoes(page)

        logger.info(
            f"[ScraperCollector] Extraído: classe={capa.get('classe')!r} "
            f"partes={len(partes)} movimentações={len(movimentacoes)}"
        )

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

    async def _extrair_capa(self, page: Page) -> dict[str, Any]:
        """Extrai campos da capa do processo via seletores ESAJ."""

        async def texto(seletor: str) -> str | None:
            el = await page.query_selector(seletor)
            if not el:
                return None
            t = (await el.inner_text()).strip()
            return t if t else None

        classe      = await texto(_SEL_CLASSE)
        assunto     = await texto(_SEL_ASSUNTO)
        vara        = await texto(_SEL_VARA)
        juiz        = await texto(_SEL_JUIZ)
        valor_raw   = await texto(_SEL_VALOR)
        data_raw    = await texto(_SEL_DATA_DIST)
        status      = await texto(_SEL_STATUS)

        logger.debug(
            f"[ScraperCollector] capa raw → classe={classe!r} vara={vara!r} "
            f"juiz={juiz!r} valor={valor_raw!r} data={data_raw!r}"
        )

        return {
            "classe": classe,
            "assunto": assunto,
            "vara": vara,
            "juiz": juiz,
            "valor_causa": _parse_valor(valor_raw),
            "data_distribuicao": _parse_date_br(data_raw),
            "status": status,
            "instancia": "1grau",
            "raw_json": None,
        }

    async def _extrair_partes(self, page: Page) -> list[Parte]:
        """
        Extrai partes processuais.
        O ESAJ TJ-SP usa td.label para o rótulo do polo e
        td.nomeParteEAdvogado para o nome + advogados.
        """
        partes: list[Parte] = []

        # Estratégia 1: seletores específicos do ESAJ
        labels = await page.query_selector_all(_SEL_PARTE_LABEL)
        nomes  = await page.query_selector_all(_SEL_PARTE_NOME)

        logger.debug(f"[ScraperCollector] Partes encontradas: labels={len(labels)} nomes={len(nomes)}")

        if labels and nomes:
            for label_el, nome_el in zip(labels, nomes):
                polo_text = (await label_el.inner_text()).strip().lower().rstrip(":")
                polo = _POLO_MAP.get(polo_text, "outro")

                # Pega apenas a primeira linha (nome da parte, não do advogado)
                nome_raw = (await nome_el.inner_text()).strip()
                nome = nome_raw.split("\n")[0].strip()
                # Remove prefixo de advogado se vier junto
                nome = re.sub(r"^(Advogad[ao]:?\s*)", "", nome, flags=re.IGNORECASE).strip()

                logger.debug(f"[ScraperCollector] Parte extraída: polo={polo!r} (label raw={polo_text!r}) nome={nome!r}")
                if nome:
                    partes.append(Parte(polo=polo, nome_tribunal=nome))
            return partes

        # Estratégia 2: fallback por tabela genérica
        rows = await page.query_selector_all(f"{_SEL_PARTES_TABLE} tr")
        logger.debug(f"[ScraperCollector] Fallback tabela partes: {len(rows)} linhas")

        for row in rows:
            cells = await row.query_selector_all("td")
            if len(cells) < 2:
                continue
            polo_text = (await cells[0].inner_text()).strip().lower().rstrip(":")
            polo = _POLO_MAP.get(polo_text, "outro")
            nome = (await cells[1].inner_text()).strip().split("\n")[0].strip()
            if nome:
                partes.append(Parte(polo=polo, nome_tribunal=nome))

        return partes

    async def _extrair_movimentacoes(self, page: Page) -> list[Movimentacao]:
        """Extrai movimentações da tabela de histórico do ESAJ."""
        movimentacoes: list[Movimentacao] = []

        # Clica em "Mostrar todas" se disponível para carregar histórico completo
        botao_todas = await page.query_selector("a#linkTodasMovimentacoes")
        if botao_todas:
            try:
                await botao_todas.click()
                await page.wait_for_load_state("networkidle", timeout=10_000)
            except Exception:
                pass  # Continua com as movimentações já visíveis

        rows = await page.query_selector_all(f"{_SEL_MOV_TABLE} tr")
        logger.debug(f"[ScraperCollector] Movimentações: {len(rows)} linhas na tabela")

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

    async def _scrape_tjrj(self, page: Page, cnj: str) -> DadosCapa:
        """
        Scraping do portal PROJUDI TJ-RJ.
        Suporta consulta pública por número CNJ unificado (1ª e 2ª instâncias).
        URL: https://www3.tjrj.jus.br/projudi/processo/consultaPublica.do
        """
        cnj_fmt = _formatar_cnj_tjrj(cnj)

        # Estratégia 1: GET direto com número formatado
        url = (
            f"{TJRJ_SEARCH_URL}"
            f"?actionType=pesquisar"
            f"&tipoConsulta=1"          # 1 = Primeira Instância
            f"&tipoNumero=UNICO"
            f"&numero={cnj_fmt}"
        )
        logger.debug(f"[ScraperCollector/TJRJ] URL 1ª instância: {url}")

        try:
            await page.goto(url, timeout=60_000, wait_until="domcontentloaded")
            await page.wait_for_load_state("networkidle", timeout=30_000)
        except PlaywrightTimeout as exc:
            raise CollectorUnavailableError(f"Timeout ao acessar TJ-RJ: {exc}") from exc

        titulo = await page.title()
        logger.debug(f"[ScraperCollector/TJRJ] Página: '{titulo}' | URL final: {page.url}")

        # Detecta CAPTCHA
        captcha = await page.query_selector(_TJRJ_SEL_CAPTCHA)
        if captcha:
            logger.warning(f"[ScraperCollector/TJRJ] CAPTCHA detectado para CNJ={cnj}")
            raise CaptchaRequiredError(f"CAPTCHA bloqueou scraping TJ-RJ CNJ={cnj}")

        # Se não encontrou na 1ª instância, tenta 2ª instância
        sem_resultado = await page.query_selector(".mensagemErro, .nenhum-resultado, #mensagemSemResultado")
        if sem_resultado:
            logger.debug(f"[ScraperCollector/TJRJ] Não encontrado em 1ª instância, tentando 2ª")
            url2 = url.replace("tipoConsulta=1", "tipoConsulta=2")
            try:
                await page.goto(url2, timeout=60_000, wait_until="domcontentloaded")
                await page.wait_for_load_state("networkidle", timeout=30_000)
            except PlaywrightTimeout as exc:
                raise CollectorUnavailableError(f"Timeout TJ-RJ 2ª instância: {exc}") from exc

        # Se ainda sem resultado, lança erro
        sem_resultado2 = await page.query_selector(".mensagemErro, .nenhum-resultado, #mensagemSemResultado")
        if sem_resultado2:
            raise CollectorUnavailableError(
                f"Processo CNJ={cnj} não encontrado no PROJUDI TJ-RJ"
            )

        capa   = await self._extrair_capa_tjrj(page)
        partes = await self._extrair_partes_tjrj(page)
        movs   = await self._extrair_movimentacoes_tjrj(page)

        logger.info(
            f"[ScraperCollector/TJRJ] Extraído: classe={capa.get('classe')!r} "
            f"partes={len(partes)} movimentações={len(movs)}"
        )

        ultima_mov: date | None = max((m.data_mov for m in movs), default=None)

        return DadosCapa(
            cnj=cnj,
            tribunal="tjrj",
            fonte="scraping",
            ultima_mov=ultima_mov,
            partes=partes,
            movimentacoes=movs,
            **capa,
        )

    async def _extrair_capa_tjrj(self, page: Page) -> dict[str, Any]:
        """Extrai campos da capa do processo no PROJUDI TJ-RJ."""

        async def texto(seletor: str) -> str | None:
            el = await page.query_selector(seletor)
            if not el:
                return None
            t = (await el.inner_text()).strip()
            return t or None

        # PROJUDI usa labels genéricos — tenta múltiplos seletores
        classe  = await texto(_TJRJ_SEL_CLASSE)
        assunto = await texto(_TJRJ_SEL_ASSUNTO)
        vara    = await texto(_TJRJ_SEL_VARA)
        juiz    = await texto(_TJRJ_SEL_JUIZ)
        valor   = await texto(_TJRJ_SEL_VALOR)
        data    = await texto(_TJRJ_SEL_DATA_DIST)
        status  = await texto(_TJRJ_SEL_STATUS)

        # Fallback: busca por texto de label em tabela genérica de capa
        if not classe:
            classe = await self._tjrj_buscar_por_label(page, ["Classe", "Classe Processual"])
        if not assunto:
            assunto = await self._tjrj_buscar_por_label(page, ["Assunto", "Assunto Principal"])
        if not vara:
            vara = await self._tjrj_buscar_por_label(page, ["Órgão Julgador", "Vara", "Juízo"])
        if not juiz:
            juiz = await self._tjrj_buscar_por_label(page, ["Magistrado", "Juiz", "Juíza"])
        if not valor:
            valor = await self._tjrj_buscar_por_label(page, ["Valor da Ação", "Valor da Causa"])
        if not data:
            data = await self._tjrj_buscar_por_label(page, ["Data de Distribuição", "Distribuído em"])
        if not status:
            status = await self._tjrj_buscar_por_label(page, ["Situação", "Status"])

        logger.debug(
            f"[ScraperCollector/TJRJ] capa raw → classe={classe!r} vara={vara!r} "
            f"juiz={juiz!r} data={data!r} status={status!r}"
        )

        return {
            "classe": classe,
            "assunto": assunto,
            "vara": vara,
            "juiz": juiz,
            "valor_causa": _parse_valor(valor),
            "data_distribuicao": _parse_date_br(data),
            "status": status,
            "instancia": "1grau",
            "raw_json": None,
        }

    async def _tjrj_buscar_por_label(self, page: Page, labels: list[str]) -> str | None:
        """
        Estratégia de fallback: percorre linhas de tabela procurando
        um <td> ou <th> que contenha o texto do label e retorna o próximo <td>.
        Cobre o padrão de tabela genérico do PROJUDI TJ-RJ.
        """
        for label in labels:
            # XPath: td/th com texto exato ou contendo o label → próximo td irmão
            el = await page.query_selector(
                f"td:has-text('{label}') + td, th:has-text('{label}') + td"
            )
            if el:
                t = (await el.inner_text()).strip()
                if t:
                    return t
        return None

    async def _extrair_partes_tjrj(self, page: Page) -> list[Parte]:
        """
        Extrai partes do PROJUDI TJ-RJ.
        O PROJUDI exibe partes em tabela com colunas: Tipo | Nome | CPF/CNPJ
        """
        partes: list[Parte] = []

        # Tenta seletor direto primeiro
        rows = await page.query_selector_all(f"{_TJRJ_SEL_PARTES} tr")

        # Fallback: qualquer tabela que contenha "Autor" ou "Réu" ou "Requerente"
        if not rows:
            tables = await page.query_selector_all("table")
            for table in tables:
                texto_tabela = (await table.inner_text()).lower()
                if any(polo in texto_tabela for polo in ["autor", "réu", "requerente", "exequente"]):
                    rows = await table.query_selector_all("tr")
                    if rows:
                        break

        logger.debug(f"[ScraperCollector/TJRJ] Partes: {len(rows)} linhas encontradas")

        for row in rows:
            cells = await row.query_selector_all("td")
            if len(cells) < 2:
                continue

            polo_text = (await cells[0].inner_text()).strip().lower().rstrip(":")
            polo = _TJRJ_POLO_MAP.get(polo_text, "outro")
            nome = (await cells[1].inner_text()).strip().split("\n")[0].strip()

            if not nome or nome.lower() in ("nome", "parte", "participante"):
                continue  # Cabeçalho da tabela

            # Documento (col 2 ou 3 se existir)
            doc: str | None = None
            if len(cells) >= 3:
                doc_raw = (await cells[2].inner_text()).strip()
                doc = re.sub(r"[^\d]", "", doc_raw) or None

            logger.debug(
                f"[ScraperCollector/TJRJ] Parte: polo={polo!r} (raw={polo_text!r}) nome={nome!r}"
            )
            partes.append(Parte(polo=polo, nome_tribunal=nome, documento=doc))

        return partes

    async def _extrair_movimentacoes_tjrj(self, page: Page) -> list[Movimentacao]:
        """
        Extrai movimentações do PROJUDI TJ-RJ.
        O PROJUDI exibe movimentos em tabela com colunas: Data | Movimento | Complemento
        """
        movimentacoes: list[Movimentacao] = []

        # Clica em "Todas as movimentações" se disponível
        for seletor in ["a:has-text('Todas')", "a:has-text('Ver todas')", "#todasMovimentacoes"]:
            botao = await page.query_selector(seletor)
            if botao:
                try:
                    await botao.click()
                    await page.wait_for_load_state("networkidle", timeout=10_000)
                except Exception:
                    pass
                break

        rows = await page.query_selector_all(f"{_TJRJ_SEL_MOVS} tr")

        # Fallback: tabela que contenha datas no formato DD/MM/YYYY
        if not rows:
            tables = await page.query_selector_all("table")
            for table in tables:
                texto = await table.inner_text()
                if re.search(r"\d{2}/\d{2}/\d{4}", texto):
                    rows = await table.query_selector_all("tr")
                    if len(rows) > 1:
                        break

        logger.debug(f"[ScraperCollector/TJRJ] Movimentações: {len(rows)} linhas encontradas")

        for row in rows:
            cells = await row.query_selector_all("td")
            if len(cells) < 2:
                continue

            data_text    = (await cells[0].inner_text()).strip()
            descricao    = (await cells[1].inner_text()).strip()
            complemento  = (await cells[2].inner_text()).strip() if len(cells) >= 3 else None

            data_mov = _parse_date_br(data_text)
            if data_mov is None or not descricao:
                continue

            movimentacoes.append(
                Movimentacao(
                    data_mov=data_mov,
                    descricao=descricao,
                    complemento=complemento or None,
                )
            )

        return movimentacoes