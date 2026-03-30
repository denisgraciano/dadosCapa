"""
collectors/escavador.py — Coletor fallback via scraping do site Escavador.

Ativado quando o scraper do tribunal é bloqueado por CAPTCHA.
Não requer autenticação — usa a página pública de busca.

O Escavador usa URLs com ID interno numérico:
  https://www.escavador.com/processos/{ID_NUMERICO}/processo-...
O ID deve ser obtido a partir da página de busca após o React renderizar.
"""

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from loguru import logger
from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeout

from collectors.base import BaseCollector, CollectorUnavailableError
from models.processo import DadosCapa, Movimentacao, Parte


ESCAVADOR_BASE_URL = "https://www.escavador.com"

_POLO_ATIVO_TEXTS   = ["polo ativo", "autor", "autora", "requerente", "exequente",
                       "impetrante", "apelante", "reclamante", "agravante", "embargante"]
_POLO_PASSIVO_TEXTS = ["polo passivo", "réu", "requerido", "executado",
                       "impetrado", "apelado", "reclamado", "agravado", "embargado"]


def _parse_date_br(value: str | None) -> date | None:
    if not value:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value.strip()[:10], fmt).date()
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
    Coletor via scraping público do Escavador (sem login/API key).
    Terceiro fallback quando o scraper do tribunal falha por CAPTCHA.
    """

    def __init__(self) -> None:
        self._user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )

    def _formatar_cnj(self, cnj: str) -> str:
        """Converte 20 dígitos para NNNNNNN-DD.AAAA.J.TT.FFFF"""
        d = re.sub(r"\D", "", cnj)
        if len(d) != 20:
            return cnj
        return f"{d[0:7]}-{d[7:9]}.{d[9:13]}.{d[13]}.{d[14:16]}.{d[16:20]}"

    async def collect(self, cnj: str) -> DadosCapa:
        logger.info(f"[EscavadorCollector] Buscando CNJ={cnj}")

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=self._user_agent,
                locale="pt-BR",
                viewport={"width": 1280, "height": 800},
            )
            page = await context.new_page()
            try:
                dados = await self._buscar_e_extrair(page, cnj)
            finally:
                await browser.close()

        return dados

    async def _buscar_e_extrair(self, page: Page, cnj: str) -> DadosCapa:
        """
        Estratégia principal: extrai partes direto da página de resultados.
        O Escavador exibe partes e dados no snippet sem precisar abrir o processo.
        Fallback: tenta abrir a página do processo se obtiver link com ID numérico.
        """
        cnj_fmt = self._formatar_cnj(cnj)
        cnj_digits = re.sub(r"\D", "", cnj)

        # ── Estratégia 1: extrai dados da página de busca ────────────────────
        url_busca = f"{ESCAVADOR_BASE_URL}/busca?q={cnj_digits}"
        logger.debug(f"[EscavadorCollector] Busca direta: {url_busca}")

        try:
            await page.goto(url_busca, timeout=60_000, wait_until="domcontentloaded")
            await page.wait_for_timeout(3000)  # aguarda React carregar snippets
        except PlaywrightTimeout as exc:
            raise CollectorUnavailableError(f"Timeout ao acessar Escavador: {exc}") from exc

        titulo = await page.title()
        logger.debug(f"[EscavadorCollector] Página: '{titulo}'")

        dados = await self._extrair_dados_da_busca(page, cnj, cnj_fmt, cnj_digits)
        if dados and dados.partes:
            logger.info(f"[EscavadorCollector] Dados extraídos da busca: {len(dados.partes)} partes")
            return dados

        # ── Estratégia 2: tenta abrir página do processo via link numérico ───
        logger.debug("[EscavadorCollector] Sem partes na busca → tentando abrir processo")
        link = await self._aguardar_link_processo(page, cnj_digits, timeout=8000)

        if link:
            try:
                await page.goto(link, timeout=60_000, wait_until="domcontentloaded")
                await self._aguardar_conteudo(page)
                logger.debug(f"[EscavadorCollector] URL processo: {page.url}")

                capa   = await self._extrair_capa(page)
                partes = await self._extrair_partes(page)
                movs   = await self._extrair_movimentacoes(page)

                logger.info(
                    f"[EscavadorCollector] Da página do processo: "
                    f"partes={len(partes)} movs={len(movs)}"
                )
                return DadosCapa(
                    cnj=cnj,
                    tribunal=self._extrair_tribunal_url(page.url),
                    fonte="scraping",
                    ultima_mov=max((m.data_mov for m in movs), default=None),
                    partes=partes,
                    movimentacoes=movs,
                    **capa,
                )
            except PlaywrightTimeout as exc:
                logger.warning(f"[EscavadorCollector] Timeout ao abrir processo: {exc}")

        # Retorna dados de capa mesmo sem partes se tiver classe/assunto
        if dados:
            return dados

        raise CollectorUnavailableError(
            f"Processo CNJ={cnj} não encontrado no Escavador"
        )

    async def _aguardar_conteudo(self, page: Page) -> None:
        """Aguarda React renderizar — tenta seletores específicos com fallback."""
        for seletor in [
            "[class*='processo']",
            "[class*='process']",
            "h1",
            "main",
        ]:
            try:
                await page.wait_for_selector(seletor, timeout=8000)
                break
            except PlaywrightTimeout:
                continue
        # Extra buffer para hidratação completa do React
        await page.wait_for_timeout(2500)

    async def _encontrar_url_processo(
        self, page: Page, cnj_fmt: str, cnj_digits: str
    ) -> str | None:
        """
        Localiza a URL real do processo no Escavador.
        O Escavador usa IDs numéricos internos: /processos/{ID}/processo-{slug}
        """

        # ── Estratégia 1: busca e captura link após React renderizar ─────────
        url_busca = f"{ESCAVADOR_BASE_URL}/busca?q={cnj_fmt}"
        logger.debug(f"[EscavadorCollector] Busca: {url_busca}")

        try:
            await page.goto(url_busca, timeout=60_000, wait_until="domcontentloaded")

            # Aguarda React carregar os resultados (pode demorar até 10s)
            link_encontrado = await self._aguardar_link_processo(page, cnj_digits, timeout=12000)
            if link_encontrado:
                logger.debug(f"[EscavadorCollector] Link encontrado na busca: {link_encontrado}")
                return link_encontrado

        except PlaywrightTimeout as exc:
            logger.debug(f"[EscavadorCollector] Timeout na busca: {exc}")

        # ── Estratégia 2: busca sem formatação ───────────────────────────────
        url_busca2 = f"{ESCAVADOR_BASE_URL}/busca?q={cnj_digits}"
        logger.debug(f"[EscavadorCollector] Busca sem formato: {url_busca2}")

        try:
            await page.goto(url_busca2, timeout=60_000, wait_until="domcontentloaded")
            link_encontrado = await self._aguardar_link_processo(page, cnj_digits, timeout=12000)
            if link_encontrado:
                logger.debug(f"[EscavadorCollector] Link encontrado (sem formato): {link_encontrado}")
                return link_encontrado
        except PlaywrightTimeout:
            pass

        # ── Estratégia 3: navega pelo campo de busca na home ─────────────────
        logger.debug(f"[EscavadorCollector] Estratégia 3 — busca via home")
        try:
            await page.goto(ESCAVADOR_BASE_URL, timeout=60_000, wait_until="domcontentloaded")
            await page.wait_for_timeout(2000)

            # Tenta preencher o campo de busca principal
            campo = await page.query_selector("input[type='search'], input[placeholder*='processo'], input[placeholder*='busca'], input[name='q']")
            if campo:
                await campo.fill(cnj_fmt)
                await campo.press("Enter")
                await page.wait_for_timeout(3000)

                link_encontrado = await self._aguardar_link_processo(page, cnj_digits, timeout=10000)
                if link_encontrado:
                    logger.debug(f"[EscavadorCollector] Link encontrado via home: {link_encontrado}")
                    return link_encontrado
        except PlaywrightTimeout:
            pass

        return None

    async def _aguardar_link_processo(
        self, page: Page, cnj_digits: str, timeout: int = 10000
    ) -> str | None:
        """
        Aguarda o React renderizar e procura links /processos/{ID_NUMERICO}/
        Tenta periodicamente até o timeout.
        """
        intervalo = 1000  # ms
        tentativas = timeout // intervalo

        for i in range(tentativas):
            # Captura todos os hrefs via JS (acessa DOM pós-hidratação)
            hrefs: list[str] = await page.evaluate("""
                () => Array.from(document.querySelectorAll('a[href]'))
                      .map(a => a.href)
                      .filter(h => /\\/processos\\/\\d+/.test(h))
            """)

            logger.debug(
                f"[EscavadorCollector] Tentativa {i+1}/{tentativas}: "
                f"{len(hrefs)} links /processos/{{ID}} encontrados"
            )

            if hrefs:
                # Prefere link que contenha dígitos do CNJ
                for href in hrefs:
                    href_digits = re.sub(r"\D", "", href)
                    if cnj_digits[:13] in href_digits or cnj_digits in href_digits:
                        return href
                # Retorna o primeiro resultado se não achou match por CNJ
                return hrefs[0]

            if i < tentativas - 1:
                await page.wait_for_timeout(intervalo)

        # Último recurso: captura qualquer link de processo no texto da página
        todos_hrefs: list[str] = await page.evaluate("""
            () => Array.from(document.querySelectorAll('a[href]'))
                  .map(a => a.href)
                  .filter(h => h.includes('/processos/'))
        """)
        logger.debug(f"[EscavadorCollector] Todos os links /processos/: {todos_hrefs[:5]}")

        return todos_hrefs[0] if todos_hrefs else None


    async def _extrair_dados_da_busca(
        self, page: Page, cnj: str, cnj_fmt: str, cnj_digits: str
    ) -> DadosCapa | None:
        """
        Extrai partes e dados diretamente da página de resultados de busca.
        O Escavador exibe no snippet: "Tem como partes envolvidas X, Y e Z"
        e nos resultados de Diário: REQUERENTE: X REQUERIDO: Y
        Não precisa abrir a página do processo — evita bloqueio de sessão.
        """
        # Captura todo o texto visível da página de resultados
        texto_pagina: str = await page.evaluate("() => document.body.innerText || ''")
        html_pagina: str  = await page.evaluate("() => document.body.innerHTML || ''")

        logger.debug(f"[EscavadorCollector] Texto busca (500 chars): {texto_pagina[:500]!r}")

        partes = self._extrair_partes_do_texto_busca(texto_pagina, cnj_digits)

        # Extrai capa dos snippets de Diário (contém CLASSE, ASSUNTO, Órgão)
        capa = self._extrair_capa_do_texto_busca(texto_pagina)

        if not partes and not capa.get("classe"):
            return None

        logger.info(
            f"[EscavadorCollector] Da busca: classe={capa.get('classe')!r} "
            f"partes={len(partes)}"
        )

        return DadosCapa(
            cnj=cnj,
            tribunal=self._extrair_tribunal_texto(texto_pagina),
            fonte="scraping",
            ultima_mov=None,
            partes=partes,
            movimentacoes=[],
            **capa,
        )

    def _extrair_partes_do_texto_busca(
        self, texto: str, cnj_digits: str
    ) -> list[Parte]:
        """
        Extrai partes de dois padrões encontrados nos resultados do Escavador:

        Padrão 1 (card do processo):
          "Tem como partes envolvidas X, Y e Z"

        Padrão 2 (snippets de Diário):
          "REQUERENTE: MOTOROLA DO BRASIL LTDA REQUERIDO: DANIEL DA SILVA FILHO"
          "AUTOR: ... RÉU: ..."  "EXEQUENTE: ... EXECUTADO: ..."
        """
        partes: list[Parte] = []
        vistos: set[str] = set()

        def add(polo: str, nome: str) -> None:
            nome = nome.strip().strip(".")
            nome = re.sub(r"\s+", " ", nome)
            if nome and len(nome) > 2 and nome.upper() not in vistos:
                vistos.add(nome.upper())
                partes.append(Parte(polo=polo, nome_tribunal=nome))
                logger.debug(f"[EscavadorCollector] Parte extraída: polo={polo!r} nome={nome!r}")

        # Padrão 1: "partes envolvidas X, Y e Z"
        m = re.search(
            r"partes? envolvidas?\s+(.+?)(?:\n|\.|$)",
            texto, re.IGNORECASE
        )
        if m:
            nomes_raw = m.group(1)
            # Separa por vírgula e "e"
            nomes = re.split(r",\s*|\s+e\s+", nomes_raw)
            for nome in nomes:
                nome = nome.strip().strip(".")
                if nome and len(nome) > 2:
                    # Sem polo definido no Escavador — marca como autor o primeiro
                    polo = "autor" if not partes else "reu"
                    add(polo, nome)

        # Padrão 2: rótulos REQUERENTE/REQUERIDO etc. em maiúsculas
        rotulos = {
            "REQUERENTE": "autor", "AUTOR": "autor", "AUTORA": "autor",
            "EXEQUENTE": "autor", "IMPETRANTE": "autor", "APELANTE": "autor",
            "RECLAMANTE": "autor", "EMBARGANTE": "autor",
            "REQUERIDO": "reu",  "RÉU": "reu",   "RÉ": "reu",
            "EXECUTADO": "reu",  "EXECUTADA": "reu", "IMPETRADO": "reu",
            "APELADO": "reu",    "RECLAMADO": "reu", "EMBARGADO": "reu",
        }

        # Monta regex dinâmica com todos os rótulos
        rotulos_re = "|".join(re.escape(r) for r in rotulos)
        padrao = re.compile(
            rf"({rotulos_re}):\s*([^\n:{{}}]+?)(?=\s*(?:{rotulos_re})|CLASSE|ASSUNTO|DECISÃO|ATO|$)",
            re.IGNORECASE
        )

        for match in padrao.finditer(texto):
            rotulo = match.group(1).upper().strip()
            nome   = match.group(2).strip()
            polo   = rotulos.get(rotulo, "outro")
            if nome:
                add(polo, nome)

        return partes

    def _extrair_capa_do_texto_busca(self, texto: str) -> dict:
        """Extrai campos de capa dos snippets de Diário na página de busca."""

        def buscar_rotulo(*rotulos: str) -> str | None:
            for rotulo in rotulos:
                m = re.search(
                    rf"{re.escape(rotulo)}[:\s]+([^\n|{{}}]+?)(?=\s*[A-Z{{}}]{{2,}}:|\n|$)",
                    texto, re.IGNORECASE
                )
                if m:
                    val = m.group(1).strip().strip("|").strip()
                    if val and len(val) < 300:
                        return val
            return None

        return {
            "classe":            buscar_rotulo("CLASSE", "Classe"),
            "assunto":           buscar_rotulo("ASSUNTO", "Assunto"),
            "vara":              buscar_rotulo("Órgão", "ORGAO", "Vara", "VARA"),
            "juiz":              buscar_rotulo("Magistrado", "Juiz", "JUIZ"),
            "valor_causa":       None,
            "data_distribuicao": None,
            "status":            None,
            "instancia":         None,
            "raw_json":          None,
        }

    def _extrair_tribunal_texto(self, texto: str) -> str:
        """Infere tribunal a partir do texto da página de busca."""
        mapa = {
            "Tribunal de Justiça do Piauí": "tjpi",
            "Tribunal de Justiça de São Paulo": "tjsp",
            "Tribunal de Justiça do Rio de Janeiro": "tjrj",
            "Tribunal de Justiça de Minas Gerais": "tjmg",
            "TJPI": "tjpi", "TJSP": "tjsp", "TJRJ": "tjrj",
            "TJMG": "tjmg", "TJRS": "tjrs", "TJPR": "tjpr",
            "TJBA": "tjba", "TJSC": "tjsc", "TJCE": "tjce",
            "TJPE": "tjpe", "TJGO": "tjgo", "TJMA": "tjma",
        }
        for nome, sigla in mapa.items():
            if nome in texto:
                return sigla
        return "desconhecido"

    def _extrair_tribunal_url(self, url: str) -> str:
        url_lower = url.lower()
        for t in ["tjsp", "tjrj", "tjmg", "tjrs", "tjpr", "tjba", "tjsc",
                  "tjce", "tjpe", "tjgo", "tjma", "tjms", "tjmt", "tjpi",
                  "tjal", "tjes", "tjrn", "tjse", "tjam", "tjpa", "tjto",
                  "trf1", "trf2", "trf3", "trf4", "trf5", "stj", "stf", "tst"]:
            if t in url_lower:
                return t
        return "desconhecido"

    async def _extrair_capa(self, page: Page) -> dict[str, Any]:
        """Extrai capa via JS varrendo dt/dd, th/td e meta tags."""
        pares: dict[str, str] = await page.evaluate("""
            () => {
                const result = {};
                document.querySelectorAll('dt').forEach(dt => {
                    const dd = dt.nextElementSibling;
                    if (dd && dd.tagName === 'DD')
                        result[dt.innerText.trim().toLowerCase()] = dd.innerText.trim();
                });
                document.querySelectorAll('tr').forEach(tr => {
                    const th = tr.querySelector('th');
                    const td = tr.querySelector('td');
                    if (th && td)
                        result[th.innerText.trim().toLowerCase()] = td.innerText.trim();
                });
                // Meta OG tags como fallback
                document.querySelectorAll('meta[property^="og:"]').forEach(m => {
                    result['og:' + m.getAttribute('property').replace('og:','')] =
                        m.getAttribute('content') || '';
                });
                return result;
            }
        """)

        def buscar(*chaves: str) -> str | None:
            for chave in chaves:
                for k, v in pares.items():
                    if chave in k and v and len(v) < 500:
                        return v.split("\n")[0].strip()
            return None

        # Tenta extrair título da página como fallback para classe
        titulo_pagina: str = await page.evaluate("() => document.title || ''")
        logger.debug(f"[EscavadorCollector] Título: {titulo_pagina!r} | Pares: {len(pares)}")

        classe  = buscar("classe", "tipo")
        assunto = buscar("assunto", "matéria")
        vara    = buscar("vara", "órgão julgador", "órgão", "juízo")
        juiz    = buscar("magistrado", "juiz", "juíza", "relator")
        data    = buscar("data de início", "distribuição", "data início", "ajuizamento")
        status  = buscar("situação", "status", "fase")
        valor   = buscar("valor da causa", "valor da ação", "valor")

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
        """Extrai partes detectando seções polo ativo/passivo no DOM."""
        partes: list[Parte] = []

        # JS: varre headings procurando polo ativo/passivo e coleta nomes
        resultado: list[dict] = await page.evaluate("""
            () => {
                const partes = [];
                const ativo_keys   = ['polo ativo','autor','autora','requerente','exequente','impetrante','apelante','reclamante','agravante','embargante'];
                const passivo_keys = ['polo passivo','réu','requerido','executada','executado','impetrado','apelado','reclamado','agravado','embargado'];

                let polo = null;
                const todos = Array.from(document.querySelectorAll('*'));

                for (const el of todos) {
                    // Só elementos folha ou quase folha com texto curto
                    if (el.children.length > 3) continue;
                    const txt = (el.innerText || '').trim().toLowerCase();
                    if (!txt || txt.length > 80) continue;

                    if (ativo_keys.some(k => txt === k || txt.startsWith(k + ':'))) {
                        polo = 'autor'; continue;
                    }
                    if (passivo_keys.some(k => txt === k || txt.startsWith(k + ':'))) {
                        polo = 'reu'; continue;
                    }

                    // Links /sobre/ e /nomes/ são nomes de partes
                    const links = el.querySelectorAll('a[href*="/sobre/"], a[href*="/nomes/"]');
                    if (links.length > 0 && polo) {
                        links.forEach(link => {
                            const nome = link.innerText.trim();
                            if (nome && nome.length > 2)
                                partes.push({polo, nome});
                        });
                    }
                }
                return partes;
            }
        """)

        vistos: set[str] = set()
        for item in resultado:
            nome = item.get("nome", "").strip()
            polo = item.get("polo", "outro")
            chave = f"{polo}|{nome.upper()}"
            if nome and chave not in vistos:
                vistos.add(chave)
                partes.append(Parte(polo=polo, nome_tribunal=nome))
                logger.debug(f"[EscavadorCollector] Parte: polo={polo!r} nome={nome!r}")

        # Fallback textual
        if not partes:
            partes = await self._extrair_partes_texto(page)

        logger.debug(f"[EscavadorCollector] Total partes: {len(partes)}")
        return partes

    async def _extrair_partes_texto(self, page: Page) -> list[Parte]:
        """Fallback: varre body.innerText detectando polo + nomes."""
        partes: list[Parte] = []
        texto: str = await page.evaluate("() => document.body.innerText || ''")
        linhas = [l.strip() for l in texto.split("\n") if l.strip()]

        polo_atual: str | None = None
        for linha in linhas:
            ll = linha.lower()
            if any(t in ll for t in _POLO_ATIVO_TEXTS) and len(linha) < 40:
                polo_atual = "autor"
                continue
            if any(t in ll for t in _POLO_PASSIVO_TEXTS) and len(linha) < 40:
                polo_atual = "reu"
                continue
            if polo_atual:
                palavras = linha.split()
                if 1 < len(palavras) <= 8 and not any(
                    c in linha for c in [":", "http", "©", "R$", "CPF", "CNPJ", "@", "/"]
                ):
                    partes.append(Parte(polo=polo_atual, nome_tribunal=linha))
                    logger.debug(f"[EscavadorCollector] Parte texto: polo={polo_atual!r} nome={linha!r}")
        return partes

    async def _extrair_movimentacoes(self, page: Page) -> list[Movimentacao]:
        """Extrai movimentações via JS varrendo elementos com data DD/MM/YYYY."""
        movimentacoes: list[Movimentacao] = []

        try:
            botao = await page.query_selector(
                "button:has-text('Ver todas'), a:has-text('Ver todas'), "
                "button:has-text('Mais'), [class*='carregar-mais']"
            )
            if botao:
                await botao.click()
                await page.wait_for_timeout(2000)
        except Exception:
            pass

        items: list[dict] = await page.evaluate("""
            () => {
                const movs = [];
                const reData = /\\d{2}\\/\\d{2}\\/\\d{4}/;
                const sels = ['[class*="movimentacao"]','[class*="andamento"]',
                              '[class*="timeline"]','[class*="movimento"]','li'];
                let elements = [];
                for (const sel of sels) {
                    elements = Array.from(document.querySelectorAll(sel));
                    if (elements.length > 2) break;
                }
                elements.forEach(el => {
                    const texto = (el.innerText || '').trim();
                    const match = texto.match(reData);
                    if (match) movs.push({data: match[0], texto});
                });
                return movs;
            }
        """)

        for item in items:
            data_mov = _parse_date_br(item.get("data"))
            texto = item.get("texto", "")
            descricao = re.sub(r"^\d{2}/\d{2}/\d{4}\s*", "", texto).strip()
            descricao = descricao.split("\n")[0].strip()
            if data_mov and descricao:
                movimentacoes.append(Movimentacao(data_mov=data_mov, descricao=descricao))

        return movimentacoes