"""
pipeline/orquestrador.py — Pipeline principal de sincronização de processos judiciais.
Itera sobre a lista de CNJs, aciona coletores, persiste dados e valida partes com IA.
"""

from datetime import date
from typing import Any

from loguru import logger

from ai.validador_partes import ValidadorPartes
from collectors.base import CaptchaRequiredError, CollectorUnavailableError
from collectors.escavador import EscavadorCollector
from collectors.datajud import DatajudCollector, TRIBUNAL_MAP, _extract_tribunal_code
from collectors.scraper import ScraperCollector
from models.processo import DadosCapa, Parte
from models.validacao import ResultadoValidacao
from repository.movimentacao_repo import MovimentacaoRepository
from repository.processo_repo import ProcessoRepository
from repository.validacao_repo import ValidacaoRepository


class Orquestrador:
    """
    Orquestra o pipeline completo de sincronização para uma lista de CNJs.
    Cada CNJ passa pelas etapas: coleta → persistência → validação IA.
    """

    def __init__(
        self,
        db_config: dict[str, Any],
        partes_sistema: list[dict[str, Any]],
    ) -> None:
        self._db_config = db_config
        self._partes_sistema = partes_sistema

        self._processo_repo = ProcessoRepository(db_config)
        self._mov_repo = MovimentacaoRepository(db_config)
        self._validacao_repo = ValidacaoRepository(db_config)
        self._validador = ValidadorPartes()

    async def executar(self, cnjs: list[str]) -> None:
        """Executa o pipeline para todos os CNJs informados."""
        logger.info(f"Iniciando sync para {len(cnjs)} CNJ(s)")

        for cnj in cnjs:
            try:
                await self._processar_cnj(cnj)
            except Exception as exc:
                # Falha em um CNJ não interrompe os demais
                logger.error(f"Erro não tratado ao processar CNJ={cnj}: {exc}")

        logger.info("Sync concluído.")

    async def _processar_cnj(self, cnj: str) -> None:
        """Pipeline completo para um único CNJ."""
        logger.info(f"── Iniciando CNJ={cnj}")

        # ── Etapa 1: Coleta de dados ────────────────────────────────────────
        ultima_mov = self._processo_repo.buscar_ultima_mov(cnj)
        dados = await self._coletar(cnj, ultima_mov)

        if dados is None:
            logger.warning(f"CNJ={cnj} ignorado — sem dados disponíveis")
            return

        # ── Etapa 2: Persistência da capa ───────────────────────────────────
        processo_id = self._processo_repo.upsert_processo(dados)
        logger.info(f"Capa persistida │ processo_id={processo_id} │ fonte={dados.fonte}")

        # ── Etapa 3: Movimentações (dedup por hash) ──────────────────────────
        total_coletadas = len(dados.movimentacoes)
        inseridas = self._mov_repo.inserir_novas(processo_id, dados.movimentacoes)
        logger.info(f"Movimentações: {inseridas} novas de {total_coletadas} coletadas")

        if dados.ultima_mov:
            self._processo_repo.atualizar_ultima_mov(processo_id, dados.ultima_mov)

        # ── Etapa 4: Partes e validação IA ──────────────────────────────────
        logger.info(
            f"Partes coletadas: {len(dados.partes)} total │ "
            f"polos: { {p.polo for p in dados.partes} if dados.partes else 'nenhuma' }"
        )
        partes_autor_reu = [p for p in dados.partes if p.polo in ("autor", "reu")]
        if partes_autor_reu:
            logger.info(f"Inserindo {len(dados.partes)} partes e validando {len(partes_autor_reu)} autor/réu")
            parte_ids = self._processo_repo.inserir_partes(processo_id, dados.partes)
            logger.info(f"IDs inseridos/existentes: {parte_ids}")
            # Mapeia apenas autor/réu para validação — preserva a ordem
            ids_autor_reu = self._filtrar_ids_autor_reu(dados.partes, parte_ids)
            await self._validar_partes(processo_id, partes_autor_reu, ids_autor_reu)
        else:
            logger.warning(
                f"Nenhuma parte com polo autor/réu encontrada para CNJ={cnj} "
                f"— partes presentes: {[(p.polo, p.nome_tribunal) for p in dados.partes]}"
            )

    async def _coletar(self, cnj: str, ultima_mov: date | None) -> DadosCapa | None:
        """
        Tenta coleta primária via DatajudCollector.
        Aciona ScraperCollector como fallback em dois cenários:
          1. DatajudCollector lança CollectorUnavailableError (fonte inacessível)
          2. DataJud retornou dados mas sem partes — LGPD ou tribunal não transmite
        """
        dados_datajud: DadosCapa | None = None

        # ── Coleta primária ──────────────────────────────────────────────────
        try:
            collector = DatajudCollector(ultima_mov=ultima_mov)
            dados_datajud = await collector.collect(cnj)
        except CollectorUnavailableError as exc:
            logger.warning(f"DatajudCollector falhou para CNJ={cnj}: {exc} → ativando fallback")

        # ── Verifica se precisa de fallback para partes ──────────────────────
        if dados_datajud is not None:
            tem_partes = any(p.polo in ("autor", "reu") for p in dados_datajud.partes)
            if not tem_partes:
                logger.warning(
                    f"DataJud não retornou partes para CNJ={cnj} "
                    f"(LGPD ou tribunal não transmite) → buscando partes via scraper"
                )
                dados_datajud = await self._complementar_partes_scraper(cnj, dados_datajud)
            return dados_datajud

        # ── Fallback total: scraper substitui DataJud inteiramente ───────────
        return await self._coletar_via_scraper(cnj)

    async def _complementar_partes_scraper(
        self, cnj: str, dados_base: DadosCapa
    ) -> DadosCapa:
        """
        Usa o scraper para complementar as partes quando o DataJud não as retornou.
        Em caso de CAPTCHA, aciona o EscavadorCollector como terceiro fallback.
        Mantém sempre capa e movimentações do DataJud (fonte primária).
        """
        try:
            codigo = _extract_tribunal_code(cnj)
            tribunal = TRIBUNAL_MAP.get(codigo, "desconhecido")
            scraper = ScraperCollector(tribunal=tribunal)
            dados_scraper = await scraper.collect(cnj)

            if dados_scraper.partes:
                logger.info(
                    f"Partes complementadas via scraper para CNJ={cnj}: "
                    f"{len(dados_scraper.partes)} parte(s) encontrada(s)"
                )
                return dados_base.model_copy(update={"partes": dados_scraper.partes})
            else:
                logger.warning(f"Scraper não encontrou partes para CNJ={cnj} → tentando Escavador")
                return await self._complementar_partes_escavador(cnj, dados_base)

        except CaptchaRequiredError as exc:
            logger.warning(
                f"CAPTCHA no scraper para CNJ={cnj}: {exc} → ativando Escavador como fallback"
            )
            return await self._complementar_partes_escavador(cnj, dados_base)
        except CollectorUnavailableError as exc:
            logger.warning(f"Scraper indisponível para CNJ={cnj}: {exc} → tentando Escavador")
            return await self._complementar_partes_escavador(cnj, dados_base)

    async def _complementar_partes_escavador(
        self, cnj: str, dados_base: DadosCapa
    ) -> DadosCapa:
        """
        Terceiro fallback: busca partes via scraping público do Escavador.
        Não requer autenticação — acessa a página pública de busca.
        """
        try:
            collector = EscavadorCollector()
            dados_esc = await collector.collect(cnj)

            if dados_esc.partes:
                logger.info(
                    f"Partes complementadas via Escavador para CNJ={cnj}: "
                    f"{len(dados_esc.partes)} parte(s) encontrada(s)"
                )
                return dados_base.model_copy(update={"partes": dados_esc.partes})
            else:
                logger.warning(f"Escavador também não retornou partes para CNJ={cnj}")
                return dados_base

        except CollectorUnavailableError as exc:
            logger.warning(f"Escavador indisponível para CNJ={cnj}: {exc}")
            return dados_base

    async def _coletar_via_scraper(self, cnj: str) -> DadosCapa | None:
        """
        Fallback total — scraper substitui DataJud inteiramente.
        Se o scraper não suportar o tribunal ou receber CAPTCHA,
        aciona o Escavador como terceiro fallback.
        """
        try:
            codigo = _extract_tribunal_code(cnj)
            tribunal = TRIBUNAL_MAP.get(codigo, "desconhecido")
            scraper = ScraperCollector(tribunal=tribunal)
            return await scraper.collect(cnj)
        except CaptchaRequiredError as exc:
            logger.warning(
                f"Scraper bloqueado por CAPTCHA para CNJ={cnj}: {exc} → tentando Escavador"
            )
            return await self._coletar_via_escavador(cnj)
        except CollectorUnavailableError as exc:
            logger.warning(
                f"Scraper indisponível para CNJ={cnj}: {exc} → tentando Escavador"
            )
            return await self._coletar_via_escavador(cnj)

    async def _coletar_via_escavador(self, cnj: str) -> DadosCapa | None:
        """Coleta completa via Escavador quando DataJud e scraper falharam."""
        try:
            logger.info(f"[Escavador] Coleta completa para CNJ={cnj}")
            collector = EscavadorCollector()
            return await collector.collect(cnj)
        except CollectorUnavailableError as exc:
            logger.error(f"Escavador também indisponível para CNJ={cnj}: {exc}")
            return None

    async def _validar_partes(
        self,
        processo_id: int,
        partes: list[Parte],
        parte_ids: list[int],
    ) -> None:
        """Valida cada parte autor/réu contra as partes do sistema interno via IA."""
        for parte, parte_id in zip(partes, parte_ids):
            resultado_ia = self._validador.validar(
                nome_tribunal=parte.nome_tribunal,
                partes_sistema=self._partes_sistema,
            )

            # Identifica nome correspondente no sistema (se houver match)
            nome_sistema: str | None = None
            idx = resultado_ia.get("indice_match")
            if idx is not None and 0 <= idx < len(self._partes_sistema):
                nome_sistema = self._partes_sistema[idx].get("nome")

            resultado = ResultadoValidacao(
                processo_id=processo_id,
                parte_id=parte_id,
                polo=parte.polo,  # type: ignore[arg-type]
                nome_tribunal=parte.nome_tribunal,
                nome_sistema=nome_sistema,
                score_ia=resultado_ia["score"],
                motivo_ia=resultado_ia.get("motivo"),
            )

            self._validacao_repo.inserir(resultado)
            self._log_validacao(parte.nome_tribunal, nome_sistema, resultado)

    def _log_validacao(
        self,
        nome_tribunal: str,
        nome_sistema: str | None,
        resultado: ResultadoValidacao,
    ) -> None:
        """Emite log estruturado do resultado de validação com ícone por status."""
        icone = {"confirmado": "✓", "revisao": "⚠", "sem_match": "✗"}.get(resultado.status, "?")
        nome_sys = nome_sistema or "—"
        logger.info(
            f"{icone} {resultado.status.upper()} '{nome_tribunal}' → '{nome_sys}'"
            f" │ score={resultado.score_ia}"
        )

    @staticmethod
    def _filtrar_ids_autor_reu(partes: list[Parte], ids: list[int]) -> list[int]:
        """
        Retorna apenas os IDs correspondentes às partes de polo autor/réu,
        mantendo o alinhamento com a lista filtrada.
        """
        return [
            pid
            for parte, pid in zip(partes, ids)
            if parte.polo in ("autor", "reu")
        ]