"""
sync_os_suprimentos.py
======================
Roda no GitHub Actions (.github/workflows/sync-os-suprimentos.yml).

Lê na réplica do SAP (Postgres/HANA) as PEÇAS PEDIDAS POR ORDEM DE SERVIÇO —
itens de compra (RC/PC) e de reserva de cada OS de Oficina/Preparação de
BHZ/BET — e envia para o webhook do app:

  POST {APP_BASE_URL}/api/public/hooks/sync-suprimentos-data
  Header: x-webhook-secret: {SYNC_WEBHOOK_SECRET}
  Body:   { "started_at": "...", "rows": [ {...} ] }

Esse bloco morava dentro do sync_patio_total.py. Foi separado para que esta
leitura — a mais pesada e a mais sensível à carga da réplica — tenha agenda,
limite de tempo e retry próprios, sem travar nem pintar de vermelho o sync do
pátio (UCA / SAP OS / Equipamentos). A query é a mesma que rodava lá.

Env (GitHub Secrets):
  HANA_DB_HOST/PORT/USER/PASSWORD/NAME (ou SAP_DB_*)
  SYNC_WEBHOOK_SECRET, APP_BASE_URL (opcional)
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError

import psycopg2
import psycopg2.extras

# Secrets colados no GitHub às vezes vêm com um espaço ou uma quebra de linha no
# fim (um "enter" sobrando), o que faz a autenticação no SAP falhar com
# `password authentication failed for user "usuario\n"`. Removemos espaços e
# quebras de TODAS as variáveis de ambiente antes de qualquer uso.
for _k, _v in list(os.environ.items()):
    if isinstance(_v, str) and _v != _v.strip():
        os.environ[_k] = _v.strip()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SAP_DB_HOST = os.environ.get("HANA_DB_HOST") or os.environ.get("SAP_DB_HOST") or ""
_sap_port_raw = os.environ.get("HANA_DB_PORT") or os.environ.get("SAP_DB_PORT")
SAP_DB_PORT = int(_sap_port_raw) if _sap_port_raw else 5432
SAP_DB_USER = os.environ.get("HANA_DB_USER") or os.environ.get("SAP_DB_USER") or ""
SAP_DB_PASSWORD = os.environ.get("HANA_DB_PASSWORD") or os.environ.get("SAP_DB_PASSWORD") or ""
SAP_DB_NAME = os.environ.get("HANA_DB_NAME") or os.environ.get("SAP_DB_NAME") or ""

APP_BASE_URL = (os.environ.get("APP_BASE_URL") or "https://gestaofilialbh.lovable.app").rstrip("/")
WEBHOOK_SECRET = os.environ.get("SYNC_WEBHOOK_SECRET") or ""

WEBHOOK_PATH = "/api/public/hooks/sync-suprimentos-data"

if not all([SAP_DB_HOST, SAP_DB_USER, SAP_DB_PASSWORD, SAP_DB_NAME]):
    print("ERRO: HANA_DB_* (ou SAP_DB_*) não definidos", file=sys.stderr)
    sys.exit(2)
if not WEBHOOK_SECRET:
    print("ERRO: SYNC_WEBHOOK_SECRET não definido", file=sys.stderr)
    sys.exit(2)

# ---------------------------------------------------------------------------
# QUERY — itens (compra + reserva) da última OS de Oficina/Preparação por ativo.
# Idêntica à que rodava no bloco SUPRIMENTOS do sync_patio_total.py.
# ---------------------------------------------------------------------------
SUPRIMENTOS_QUERY = """
WITH ultima_os AS (
    SELECT
        n_equipamento AS ativo,
        TRIM(LEADING '0' FROM n_ordem) AS ordem_tratada,
        LPAD(TRIM(n_ordem), 12, '0') AS ordem_sap
    FROM (
        SELECT
            n_equipamento,
            n_ordem,
            ROW_NUMBER() OVER(
                PARTITION BY n_equipamento,
                CASE
                    WHEN cod_tipo_atividade = 'PRP' OR tipo_atividade ILIKE '%%prep%%' THEN 'Preparação'
                    WHEN cod_tipo_atividade = 'OFI' OR tipo_atividade ILIKE '%%ofi%%' THEN 'Oficina'
                END
                ORDER BY data_criacao DESC
            ) as rn
        FROM pm_ordem_manutencao_cabecalho_v2
        WHERE (cod_centro_trabalho LIKE '%%BHZ%%' OR cod_centro_trabalho LIKE '%%BET%%')
          AND data_criacao >= '2024-01-01'
          AND (cod_tipo_atividade IN ('PRP', 'OFI') OR tipo_atividade ILIKE '%%prep%%' OR tipo_atividade ILIKE '%%ofi%%')
    ) sub
    WHERE rn = 1
),
itens_brutos AS (
    SELECT
        os.ativo,
        os.ordem_tratada AS ordem,
        LTRIM(TRIM(E.MATNR), '0') AS cod_sap,
        E.TXZ01 AS desc_compra_direta,
        E.MENGE AS qtd_req,
        LTRIM(TRIM(P.EBELN), '0') AS pedido,
        CAST(NULL AS VARCHAR) AS num_reserva,
        'compra'::text AS origem,
        E.KNTTP AS knttp,
        TRIM(COALESCE(RX.VORNR, '')) AS num_operacao
    FROM ultima_os os
    JOIN EBKN ACC ON ACC.AUFNR = os.ordem_sap
    JOIN EBAN E   ON E.BANFN = ACC.BANFN AND E.BNFPO = ACC.BNFPO
    LEFT JOIN EKPO P ON P.BANFN = E.BANFN AND P.BNFPO = E.BNFPO AND COALESCE(TRIM(P.LOEKZ),'') <> 'L'
    LEFT JOIN LATERAL (
        SELECT MAX(TRIM(R2.VORNR)) AS VORNR
        FROM RESB R2
        WHERE R2.AUFNR = os.ordem_sap
          AND LTRIM(TRIM(R2.MATNR), '0') = LTRIM(TRIM(E.MATNR), '0')
          AND TRIM(COALESCE(R2.VORNR,'')) <> ''
    ) RX ON TRUE


    UNION ALL

    SELECT
        os.ativo,
        os.ordem_tratada AS ordem,
        LTRIM(TRIM(R.MATNR), '0') AS cod_sap,
        CAST(NULL AS VARCHAR) AS desc_compra_direta,
        R.BDMNG AS qtd_req,
        LTRIM(TRIM(P.EBELN), '0') AS pedido,
        CASE
            WHEN R.POSTP = 'N' THEN CAST(NULL AS VARCHAR)
            ELSE LTRIM(TRIM(R.RSNUM), '0')
        END AS num_reserva,
        CASE
            WHEN R.POSTP = 'N' THEN 'compra'::text
            ELSE 'reserva'::text
        END AS origem,
        E.KNTTP AS knttp,
        TRIM(COALESCE(OPR.VORNR, R.VORNR, '')) AS num_operacao
    FROM ultima_os os
    JOIN RESB R   ON R.AUFNR = os.ordem_sap
    LEFT JOIN EBAN E ON E.BANFN = R.BANFN AND E.BNFPO = R.BNFPO
    LEFT JOIN EKPO P ON P.BANFN = E.BANFN AND P.BNFPO = E.BNFPO AND COALESCE(TRIM(P.LOEKZ),'') <> 'L'
    LEFT JOIN AFVC OPR ON OPR.AUFPL = R.AUFPL AND OPR.APLZL = R.APLZL
    WHERE (R.XLOEK IS NULL OR TRIM(R.XLOEK) = '')
),
itens_deduplicados AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ordem, cod_sap, num_operacao, origem
            ORDER BY
                CASE WHEN pedido IS NOT NULL THEN 1 ELSE 2 END,
                CASE WHEN num_reserva IS NOT NULL THEN 1 ELSE 2 END
        ) AS rn
    FROM itens_brutos
    WHERE cod_sap IS NOT NULL AND cod_sap <> ''
)
SELECT
    itens.ativo         AS ativo,
    itens.ordem         AS ordem,
    itens.cod_sap       AS cod_sap,
    COALESCE(M.MAKTX, itens.desc_compra_direta, 'Sem Descrição') AS descricao,
    itens.qtd_req       AS qtd_req,
    itens.num_reserva   AS num_reserva,
    itens.pedido        AS pedido,
    itens.origem        AS origem,
    itens.num_operacao  AS num_operacao,
    NULLIF(TRIM(COALESCE(itens.knttp,'')),'') AS knttp,
    CASE
        WHEN COALESCE(TRIM(itens.knttp),'') = '' THEN 'estoque'
        WHEN TRIM(itens.knttp) = 'K' THEN 'centro_custo'
        ELSE 'consumo'
    END AS destinacao
FROM itens_deduplicados itens
LEFT JOIN MAKT M ON LTRIM(TRIM(M.MATNR), '0') = itens.cod_sap AND M.SPRAS IN ('P', 'PT')
WHERE itens.rn = 1
ORDER BY itens.ativo, itens.ordem, itens.num_operacao;

"""


# ---------------------------------------------------------------------------
# Helpers (mesma serialização do sync_patio_total: datas "ingênuas" = Brasília)
# ---------------------------------------------------------------------------
TZ_BR = timezone(timedelta(hours=-3))


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def assume_br(v: datetime) -> datetime:
    return v.replace(tzinfo=TZ_BR) if v.tzinfo is None else v


def jsonable(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, datetime):
        return assume_br(v).isoformat()
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day, tzinfo=TZ_BR).isoformat()
    return str(v)


def to_payload(row: dict) -> dict:
    return {k: jsonable(v) for k, v in row.items()}


def _erro_transitorio_replica(err: Exception) -> bool:
    """A réplica do SAP (hot standby) cancela leituras longas enquanto aplica a
    replicação ("conflict with recovery") ou derruba a conexão. São erros
    passageiros: vale esperar um pouco e tentar de novo."""
    txt = str(err).lower()
    return (
        "conflict with recovery" in txt
        or "canceling statement due to conflict" in txt
        or "terminating connection due to conflict" in txt
        or "server closed the connection unexpectedly" in txt
    )


def fetch_postgres(query: str, attempts: int = 3) -> list[dict]:
    ultimo: Exception | None = None
    for i in range(attempts):
        conn = None
        try:
            conn = psycopg2.connect(
                host=SAP_DB_HOST,
                port=SAP_DB_PORT,
                user=SAP_DB_USER,
                password=SAP_DB_PASSWORD,
                dbname=SAP_DB_NAME,
                connect_timeout=20,
                sslmode="require",
                application_name="sync_os_suprimentos",
                # Limite de 5 min por consulta. Em réplica normal ela leva ~1 min;
                # passou disso, corta em vez de pendurar. O corte por tempo NÃO
                # dispara retry (só os erros passageiros acima disparam).
                options="-c statement_timeout=300000",
            )
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query)
                return [dict(r) for r in cur.fetchall()]
        except Exception as e:  # noqa: BLE001
            ultimo = e
            if not _erro_transitorio_replica(e) or i == attempts - 1:
                raise
            espera = 10 * (i + 1)
            print(
                f"  ! réplica cancelou/derrubou a leitura (tentativa {i + 1}/{attempts}); "
                f"aguardando {espera}s e repetindo…",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(espera)
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass
    raise ultimo  # type: ignore[misc]


def post_json(path: str, body: dict) -> dict:
    url = f"{APP_BASE_URL}{path}"
    data = json.dumps(body).encode("utf-8")
    req = urlrequest.Request(
        url,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "x-webhook-secret": WEBHOOK_SECRET,
            "User-Agent": "sync_os_suprimentos/1.0",
        },
    )
    try:
        with urlrequest.urlopen(req, timeout=180) as resp:
            txt = resp.read().decode("utf-8", errors="replace")
            try:
                return json.loads(txt)
            except json.JSONDecodeError:
                return {"ok": False, "raw": txt[:500]}
    except HTTPError as e:
        body_txt = e.read().decode("utf-8", errors="replace") if e.fp else ""
        raise RuntimeError(f"HTTP {e.code} em {path}: {body_txt[:500]}")
    except URLError as e:
        raise RuntimeError(f"Falha de rede em {path}: {e}")


def report_failure(started_at: str, exc: Exception) -> None:
    """Avisa o app para o painel não ficar preso em 'running'."""
    try:
        post_json(WEBHOOK_PATH, {"started_at": started_at, "error": str(exc)})
    except Exception as report_exc:  # noqa: BLE001
        print(f"[AVISO] não consegui registrar falha no app: {report_exc}", file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    started_at = now_utc_iso()
    print(f"[{started_at}] sync_os_suprimentos — iniciando", flush=True)
    print(f"  SAP: {SAP_DB_USER}@{SAP_DB_HOST}:{SAP_DB_PORT}/{SAP_DB_NAME}", flush=True)
    print(f"  Destino: {APP_BASE_URL}{WEBHOOK_PATH}", flush=True)

    try:
        rows = fetch_postgres(SUPRIMENTOS_QUERY)
        print(f"  Linhas lidas: {len(rows)}", flush=True)
        payload = {"started_at": started_at, "rows": [to_payload(r) for r in rows]}
        result = post_json(WEBHOOK_PATH, payload)
        if not result.get("ok"):
            raise RuntimeError(f"app respondeu sem ok: {result}")
        print(f"[SUPR] ok · {result.get('message') or result}", flush=True)
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"[SUPR] erro · {exc}", file=sys.stderr, flush=True)
        report_failure(started_at, exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
