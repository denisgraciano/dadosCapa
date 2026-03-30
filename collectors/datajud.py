"""
collectors/datajud.py — Coletor primário via API REST pública do DataJud (CNJ).
Usa httpx assíncrono com retry exponencial via tenacity.
"""

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from collectors.base import BaseCollector, CollectorUnavailableError
from config import settings
from models.processo import DadosCapa, Movimentacao, Parte


# Mapeamento de código de tribunal (posições 14-15 do CNJ) para sigla do DataJud.
# Fonte: tabela oficial CNJ + índices disponíveis na API pública.
# Múltiplos códigos podem apontar para o mesmo tribunal (ex: TJ-SP usa 26, 35, 82, 83...).
TRIBUNAL_MAP: dict[str, str] = {
    # Justiça Federal — TRFs
    "01": "trf1",
    "02": "trf2",
    "03": "trf3",
    "05": "trf5",
    "06": "trf6",

    # Justiça do Trabalho — TRTs
    "04": "trt4",
    "15": "trt15",  # TRT-15 (Campinas) — também usa código 15

    # Tribunais de Justiça Estaduais
    "07": "tjap",
    "08": "tjpa",
    "09": "tjba",
    "10": "tjce",
    "11": "tjmt",
    "12": "tjms",
    "13": "tjam",
    "14": "tjro",
    "16": "tjrn",
    "17": "tjpe",
    "18": "tjpi",
    "19": "tjrj",
    "20": "tjsc",
    "21": "tjal",
    "22": "tjpi",
    "23": "tjmg",
    "24": "tjms",
    "25": "tjse",
    "26": "tjsp",   # TJ-SP — código principal (1º grau)
    "27": "tjes",
    "28": "tjse",
    "29": "tjma",
    "30": "tjpr",
    "31": "tjmg",
    "32": "tjes",
    "33": "tjrj",
    "34": "tjdf",
    "35": "tjsp",   # TJ-SP — turmas recursais / JEC
    "36": "tjap",
    "37": "tjam",
    "38": "tjmg",
    "39": "tjto",
    "40": "tjrr",
    "41": "tjpr",
    "42": "tjsc",
    "43": "tjrs",
    "44": "tjms",
    "45": "tjmt",
    "46": "tjgo",
    "47": "tjba",
    "48": "tjal",
    "49": "tjse",
    "50": "tjpe",
    "51": "tjpb",
    "52": "tjrn",
    "53": "tjce",
    "54": "tjpi",
    "55": "tjma",
    "56": "tjpa",
    "57": "tjap",
    "58": "tjam",
    "59": "tjrr",
    "60": "tjac",
    "61": "tjro",
    "62": "tjto",
    "63": "tjdf",
    "64": "tjgo",
    "65": "tjmg",
    "66": "tjes",
    "67": "tjrj",
    "68": "tjsp",   # TJ-SP — câmaras especializadas
    "69": "tjpr",
    "70": "tjrs",
    "71": "tjms",
    "72": "tjmt",
    "73": "tjba",
    "74": "tjal",
    "75": "tjse",
    "76": "tjpe",
    "77": "tjpb",
    "78": "tjrn",
    "79": "tjce",
    "80": "tjpi",
    "81": "tjma",
    "82": "tjsp",   # TJ-SP — 2ª instância / câmaras de direito privado
    "83": "tjsp",   # TJ-SP — câmaras de direito público
    "84": "tjsp",   # TJ-SP — câmaras de direito criminal
    "85": "tjsp",   # TJ-SP — câmaras especiais
    "86": "tjrj",
    "87": "tjpr",
    "88": "tjrs",
    "89": "tjmg",
    "90": "tjba",
}

DATAJUD_BASE_URL = "https://api-publica.datajud.cnj.jus.br"


def _extract_tribunal_code(cnj: str) -> str:
    """
    Extrai o código do tribunal do CNJ unificado (20 dígitos).

    Estrutura: NNNNNNN DD AAAA J TT FFFF
    Índices:   0......6 7.8 9..12 13 14.15 16..19

    J  = segmento (posição 13): 8 = Justiça Estadual
    TT = tribunal (posições 14-15): ex. 19 = TJRJ, 26 = TJSP
    """
    cnj_digits = "".join(filter(str.isdigit, cnj))
    if len(cnj_digits) != 20:
        raise CollectorUnavailableError(
            f"CNJ inválido (esperado 20 dígitos, got {len(cnj_digits)}): {cnj!r}"
        )
    return cnj_digits[14:16]


def _parse_date(value: str | None) -> date | None:
    """Tenta converter string ISO para date, retorna None em caso de falha."""
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(value[:19], fmt[:len(value[:19])]).date()
        except ValueError:
            continue
    return None


def _parse_decimal(value: Any) -> Decimal | None:
    """Converte valor para Decimal, retorna None se inválido."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


class DatajudCollector(BaseCollector):
    """Coleta dados processuais via API pública do DataJud/CNJ."""

    def __init__(self, ultima_mov: date | None = None) -> None:
        self._ultima_mov = ultima_mov

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    async def _post(self, client: httpx.AsyncClient, url: str, payload: dict) -> dict:
        """Executa POST com retry automático em caso de falha transitória."""
        try:
            response = await client.post(url, json=payload, timeout=30.0)
            response.raise_for_status()
            return response.json()
        except httpx.TimeoutException as exc:
            raise CollectorUnavailableError(f"Timeout ao consultar DataJud: {exc}") from exc
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code >= 500:
                raise CollectorUnavailableError(
                    f"DataJud retornou {exc.response.status_code}"
                ) from exc
            raise CollectorUnavailableError(
                f"Erro HTTP {exc.response.status_code} ao consultar DataJud"
            ) from exc
        except httpx.RequestError as exc:
            raise CollectorUnavailableError(f"Erro de rede ao consultar DataJud: {exc}") from exc

    async def collect(self, cnj: str) -> DadosCapa:
        """Coleta dados completos de um processo via DataJud."""
        codigo = _extract_tribunal_code(cnj)
        tribunal = TRIBUNAL_MAP.get(codigo)

        if not tribunal:
            raise CollectorUnavailableError(
                f"Tribunal com código '{codigo}' não mapeado para CNJ {cnj}"
            )

        # Endpoint /_search conforme documentação e validado no Postman
        url = f"{DATAJUD_BASE_URL}/api_publica_{tribunal}/_search"
        payload = {
            "query": {
                "match": {"numeroProcesso": cnj}
            }
        }

        headers = {
            "Authorization": f"APIKey {settings.datajud_api_key}",
            "Content-Type": "application/json",
        }

        logger.info(f"[DatajudCollector] Consultando CNJ={cnj} → tribunal={tribunal}")

        async with httpx.AsyncClient(headers=headers) as client:
            data = await self._post(client, url, payload)

        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            raise CollectorUnavailableError(
                f"DataJud não retornou resultados para CNJ={cnj}"
            )

        source: dict[str, Any] = hits[0].get("_source", {})
        return self._parse_source(cnj, tribunal, source)

    def _parse_source(self, cnj: str, tribunal: str, source: dict[str, Any]) -> DadosCapa:
        """Mapeia o payload bruto do DataJud para DadosCapa."""

        # Log diagnóstico — inspeciona estrutura real do _source
        logger.debug(f"[DatajudCollector] _source keys CNJ={cnj}: {list(source.keys())}")

        partes_raw = source.get("partes", [])
        logger.debug(
            f"[DatajudCollector] partes_raw tipo={type(partes_raw).__name__} "
            f"qtd={len(partes_raw) if isinstance(partes_raw, list) else '?'} "
            f"amostra={partes_raw[:2] if isinstance(partes_raw, list) else partes_raw!r}"
        )

        partes = self._parse_partes(partes_raw)
        logger.info(f"[DatajudCollector] Partes extraídas={len(partes)} de raw={len(partes_raw) if isinstance(partes_raw, list) else '?'}")

        movimentacoes = self._parse_movimentacoes(source.get("movimentos", []))

        # Determina data da última movimentação coletada
        ultima_mov: date | None = None
        if movimentacoes:
            ultima_mov = max(m.data_mov for m in movimentacoes)

        return DadosCapa(
            cnj=cnj,
            tribunal=tribunal,
            classe=source.get("classe", {}).get("nome") if isinstance(source.get("classe"), dict) else source.get("classe"),
            assunto=self._extract_assunto(source.get("assuntos", [])),
            valor_causa=_parse_decimal(source.get("valorCausa")),
            vara=source.get("orgaoJulgador", {}).get("nome") if isinstance(source.get("orgaoJulgador"), dict) else source.get("orgaoJulgador"),
            juiz=source.get("magistrado"),
            instancia=self._map_instancia(source.get("grau")),
            status=source.get("situacao", {}).get("nome") if isinstance(source.get("situacao"), dict) else source.get("situacao"),
            data_distribuicao=_parse_date(source.get("dataAjuizamento")),
            fonte="datajud",
            raw_json=source,
            ultima_mov=ultima_mov,
            partes=partes,
            movimentacoes=movimentacoes,
        )

    def _extract_assunto(self, assuntos: list) -> str | None:
        """Extrai o assunto principal da lista de assuntos."""
        if not assuntos:
            return None
        primeiro = assuntos[0]
        if isinstance(primeiro, dict):
            return primeiro.get("nome")
        return str(primeiro)

    def _map_instancia(self, grau: str | None) -> str | None:
        """Converte o campo 'grau' do DataJud para o ENUM do banco."""
        mapa = {
            "G1": "1grau",
            "G2": "2grau",
            "SUP": "stj",
            "STA": "stf",
            "TST": "tst",
        }
        return mapa.get(grau or "")

    def _parse_partes(self, partes_raw: list) -> list[Parte]:
        """
        Converte lista de partes do DataJud para modelos Pydantic.
        O DataJud pode retornar polo em vários formatos:
          - string descritiva: "Autor", "Réu", "Advogado"
          - abreviado:         "AT" (ativo), "RE" (réu/passivo), "AD"
          - tipoParte:         campo alternativo em alguns tribunais
        """
        resultado: list[Parte] = []

        # Mapa amplo cobrindo todas as variações conhecidas do DataJud
        polo_map: dict[str, str] = {
            # Polo ativo / autor
            "autor":       "autor",
            "autora":      "autor",
            "at":          "autor",
            "ativo":       "autor",
            "requerente":  "autor",
            "impetrante":  "autor",
            "exequente":   "autor",
            "apelante":    "autor",
            "embargante":  "autor",
            "reclamante":  "autor",
            "recorrente":  "autor",
            # Polo passivo / réu
            "réu":         "reu",
            "reu":         "reu",
            "re":          "reu",
            "passivo":     "reu",
            "requerido":   "reu",
            "requerida":   "reu",
            "impetrado":   "reu",
            "executado":   "reu",
            "executada":   "reu",
            "apelado":     "reu",
            "apelada":     "reu",
            "embargado":   "reu",
            "reclamado":   "reu",
            "recorrido":   "reu",
            # Advogados
            "advogado":    "advogado",
            "advogada":    "advogado",
            "ad":          "advogado",
            "defensor":    "advogado",
            "defensora":   "advogado",
        }

        for item in partes_raw:
            if not isinstance(item, dict):
                continue

            # Tenta diferentes campos onde o polo pode estar
            polo_raw = (
                item.get("polo")
                or item.get("tipoParte")
                or item.get("tipoParteDescricao")
                or "outro"
            )
            polo = polo_map.get(str(polo_raw).lower().strip(), "outro")

            # Tenta diferentes campos onde o nome pode estar
            nome = (
                item.get("nome")
                or item.get("nomeParticipante")
                or item.get("nomeParte")
                or ""
            ).strip()

            if not nome:
                logger.debug(f"[DatajudCollector] Parte sem nome ignorada: {item}")
                continue

            # Extrai documento (CPF/CNPJ) — pode vir mascarado por LGPD
            doc: str | None = None
            for doc_item in item.get("documentos", []):
                if isinstance(doc_item, dict):
                    numero = doc_item.get("numero", "")
                    # Aceita mesmo mascarado — útil para log, não para match
                    if numero:
                        doc = str(numero)
                        break

            logger.debug(f"[DatajudCollector] Parte: polo={polo!r} (raw={polo_raw!r}) nome={nome!r}")
            resultado.append(Parte(polo=polo, nome_tribunal=nome, documento=doc))

        return resultado

    def _parse_movimentacoes(self, movimentos_raw: list) -> list[Movimentacao]:
        """
        Converte lista de movimentações do DataJud para modelos Pydantic.
        Filtra movimentações anteriores à ultima_mov para coleta incremental.
        """
        resultado: list[Movimentacao] = []

        for item in movimentos_raw:
            if not isinstance(item, dict):
                continue

            data_mov = _parse_date(item.get("dataHora"))
            if data_mov is None:
                continue

            # Coleta incremental: ignora movimentos já persistidos
            if self._ultima_mov and data_mov <= self._ultima_mov:
                continue

            resultado.append(
                Movimentacao(
                    data_mov=data_mov,
                    codigo_mov=str(item.get("codigo", "")) or None,
                    descricao=item.get("nome", "Sem descrição"),
                    complemento=self._extrair_complemento(item.get("complementosTabelados")),
                )
            )

        return resultado

    @staticmethod
    def _extrair_complemento(complementos: Any) -> str | None:
        """
        Extrai texto do campo complementosTabelados.
        A API retorna lista de dicts: [{"codigo": 18, "valor": "texto", ...}]
        Concatena todos os valores encontrados separados por " | ".
        """
        if not complementos or not isinstance(complementos, list):
            return None

        partes: list[str] = []
        for c in complementos:
            if isinstance(c, dict):
                texto = c.get("valor") or c.get("descricao") or c.get("nome")
                if texto:
                    partes.append(str(texto).strip())
            elif isinstance(c, str) and c.strip():
                partes.append(c.strip())

        return " | ".join(partes) if partes else None