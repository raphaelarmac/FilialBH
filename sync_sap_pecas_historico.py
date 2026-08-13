"""
sync_sap_pecas_historico.py
===========================
Roda 1x/dia no GitHub Actions. Extrai histórico AGREGADO de peças usadas em OS
do SAP (desde 2024) — uma linha por (ativo, cod_sap) com contagem de ocorrências
e quantidade total — e envia para o webhook do app:

  POST {APP_BASE_URL}/api/public/hooks/sync-sap-pecas-historico
  Header: x-webhook-secret: {SYNC_WEBHOOK_SECRET}
  Body:   { "started_at": "...", "rows": [ {...}, ... ] }
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError

import psycopg2
import psycopg2.extras

SAP_DB_HOST = os.environ.get("HANA_DB_HOST") or os.environ.get("SAP_DB_HOST") or ""
_p = os.environ.get("HANA_DB_PORT") or os.environ.get("SAP_DB_PORT")
SAP_DB_PORT = int(_p) if _p else 5432
SAP_DB_USER = os.environ.get("HANA_DB_USER") or os.environ.get("SAP_DB_USER") or ""
SAP_DB_PASSWORD = os.environ.get("HANA_DB_PASSWORD") or os.environ.get("SAP_DB_PASSWORD") or ""
SAP_DB_NAME = os.environ.get("HANA_DB_NAME") or os.environ.get("SAP_DB_NAME") or ""

APP_BASE_URL = (os.environ.get("APP_BASE_URL") or "https://gestaofilialbh.lovable.app").rstrip("/")
WEBHOOK_SECRET = os.environ.get("SYNC_WEBHOOK_SECRET") or ""

if not all([SAP_DB_HOST, SAP_DB_USER, SAP_DB_PASSWORD, SAP_DB_NAME]):
    print("ERRO: HANA_DB_* (ou SAP_DB_*) não definidos", file=sys.stderr)
    sys.exit(2)
if not WEBHOOK_SECRET:
    print("ERRO: SYNC_WEBHOOK_SECRET não definido", file=sys.stderr)
    sys.exit(2)

# Agrega no SAP: uma linha por (ativo, cod_sap), com nº de OSs distintas que
# consumiram a peça, quantidade total, última OS e última data.
QUERY = """
SELECT
    LTRIM(TRIM(AFIH.EQUNR), '0') AS ativo,
    MAX(EQKT.EQKTX) AS descricao_ativo,
    LTRIM(TRIM(RESB.MATNR), '0') AS cod_sap,
    MAX(MAKT.MAKTX) AS descricao_componente,
    MAX(MARA.MATKL) AS grupo_mercadoria,
    COUNT(DISTINCT AFIH.AUFNR) AS qtd_ocorrencias,
    SUM(RESB.BDMNG) AS qtd_total,
    MAX(LTRIM(TRIM(AFIH.AUFNR), '0')) AS ultima_ordem,
    MAX(AUFK.ERDAT) AS ultima_data
FROM AFIH
INNER JOIN RESB ON LTRIM(TRIM(AFIH.AUFNR), '0') = LTRIM(TRIM(RESB.AUFNR), '0')
LEFT JOIN EQKT ON LTRIM(TRIM(AFIH.EQUNR), '0') = LTRIM(TRIM(EQKT.EQUNR), '0')
                AND EQKT.SPRAS IN ('P','PT')
LEFT JOIN MAKT ON LTRIM(TRIM(RESB.MATNR), '0') = LTRIM(TRIM(MAKT.MATNR), '0')
                AND MAKT.SPRAS IN ('P','PT')
LEFT JOIN MARA ON LTRIM(TRIM(RESB.MATNR), '0') = LTRIM(TRIM(MARA.MATNR), '0')
LEFT JOIN AUFK ON LTRIM(TRIM(AFIH.AUFNR), '0') = LTRIM(TRIM(AUFK.AUFNR), '0')
WHERE AFIH.EQUNR IS NOT NULL AND AFIH.EQUNR <> ''
  AND RESB.MATNR IS NOT NULL AND RESB.MATNR <> ''
  AND AUFK.ERDAT >= %(ini)s AND AUFK.ERDAT <= %(fim)s
GROUP BY LTRIM(TRIM(AFIH.EQUNR), '0'), LTRIM(TRIM(RESB.MATNR), '0');
"""


def to_jsonable(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.isoformat()
    # Decimal, date, etc. viram string via default=str no json.dumps
    return v


def post_webhook(payload: dict) -> None:
    url = f"{APP_BASE_URL}/api/public/hooks/sync-sap-pecas-historico"
    data = json.dumps(payload, default=str).encode("utf-8")
    req = urlrequest.Request(
        url, data=data, method="POST",
        headers={
            "Content-Type": "application/json",
            "x-webhook-secret": WEBHOOK_SECRET,
            "User-Agent": "Mozilla/5.0 (compatible; ArmacSync/1.0)",
        },
    )
    # 5xx costuma ser timeout momentaneo do banco: tenta de novo com backoff.
    for attempt in range(1, 4):
        try:
            with urlrequest.urlopen(req, timeout=180) as resp:
                print(f"webhook {resp.status}: {resp.read()[:120].decode('utf-8','ignore')}", flush=True)
            return
        except HTTPError as e:
            body = e.read()[:400].decode('utf-8', 'ignore')
            print(f"HTTP {e.code} (tentativa {attempt}/3): {body}", file=sys.stderr, flush=True)
            if e.code < 500 or attempt == 3:
                raise
        except URLError as e:
            print(f"URLError (tentativa {attempt}/3): {e}", file=sys.stderr, flush=True)
            if attempt == 3:
                raise
        time.sleep(5 * attempt)


def _is_recovery_conflict(err: Exception) -> bool:
    """Réplica de leitura (hot standby) cancela queries longas durante replay do WAL."""
    txt = str(err).lower()
    return (
        "conflict with recovery" in txt
        or "canceling statement due to conflict" in txt
        or "terminating connection due to conflict" in txt
        or isinstance(err, psycopg2.errors.SerializationFailure)
    )


def _fetch_periodo_once(ini: str, fim: str) -> list[dict]:
    print(f"conectando no SAP em {SAP_DB_HOST}:{SAP_DB_PORT}/{SAP_DB_NAME}...", flush=True)
    conn = psycopg2.connect(
        host=SAP_DB_HOST, port=SAP_DB_PORT, user=SAP_DB_USER,
        password=SAP_DB_PASSWORD, dbname=SAP_DB_NAME,
        connect_timeout=30,
    )
    conn.autocommit = True
    try:
        print(f"executando query SAP AGREGADA período {ini}..{fim}...", flush=True)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(QUERY, {"ini": ini, "fim": fim})
            rows = cur.fetchall()
        print(f"query OK ({ini}..{fim}): {len(rows)} linhas lidas", flush=True)
        return [{k: to_jsonable(v) for k, v in r.items()} for r in rows]
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _fetch_periodo(ini: str, fim: str, attempts: int = 8) -> list[dict]:
    last = None
    for i in range(attempts):
        try:
            return _fetch_periodo_once(ini, fim)
        except Exception as e:
            last = e
            if not _is_recovery_conflict(e) or i == attempts - 1:
                raise
            wait = min(120, 15 * (i + 1))
            print(f"! conflito com recovery na réplica SAP ({ini}..{fim}, tentativa {i + 1}/{attempts}); "
                  f"aguardando {wait}s e repetindo…", flush=True)
            time.sleep(wait)
    raise last


def _periodos() -> list[tuple[str, str]]:
    """Fatia por semestre: queries mais curtas sofrem menos cancelamento na réplica."""
    ano_atual = datetime.now(timezone.utc).year
    out: list[tuple[str, str]] = []
    for ano in range(2024, ano_atual + 1):
        out.append((f"{ano}0101", f"{ano}0630"))
        out.append((f"{ano}0701", f"{ano}1231"))
    return out


def fetch_sap_rows() -> list[dict]:
    todas: list[dict] = []
    for ini, fim in _periodos():
        todas.extend(_fetch_periodo(ini, fim))
    print(f"total lido do SAP: {len(todas)} linhas", flush=True)
    return todas


def main():
    started_at = datetime.now(timezone.utc).isoformat()
    print(f"iniciando sync_sap_pecas_historico (agregado) em {started_at}", flush=True)
    print(f"destino webhook: {APP_BASE_URL}/api/public/hooks/sync-sap-pecas-historico", flush=True)

    print("testando webhook antes da query...", flush=True)
    try:
        post_webhook({
            "started_at": started_at,
            "batch_index": -1,
            "batch_total": -1,
            "is_last": False,
            "rows": [],
        })
    except Exception as e:
        print(f"ERRO no webhook antes da query: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

    # Lê TUDO do SAP primeiro (rápido) e só depois envia em batches.
    # Antes o envio acontecia com o cursor aberto: a réplica de leitura cancelava
    # a query ("conflict with recovery") enquanto esperávamos os webhooks.
    try:
        all_rows = fetch_sap_rows()
    except Exception as e:
        import traceback
        print(f"ERRO query SAP: {e!r}", file=sys.stderr, flush=True)
        traceback.print_exc()
        post_webhook({"started_at": started_at, "error": f"query/envio SAP: {e}"})
        sys.exit(1)

    CHUNK = 500
    total = 0
    batch_index = 0
    try:
        for start in range(0, len(all_rows), CHUNK):
            buffer = all_rows[start:start + CHUNK]
            post_webhook({
                "started_at": started_at,
                "batch_index": batch_index,
                "batch_total": -1,
                "is_last": False,
                "rows": buffer,
            })
            total += len(buffer)
            print(f"batch {batch_index} enviado, linhas_batch={len(buffer)}, total={total}", flush=True)
            batch_index += 1
        # último batch (vazio) sinaliza fim
        post_webhook({
            "started_at": started_at,
            "batch_index": batch_index,
            "batch_total": batch_index + 1,
            "is_last": True,
            "rows": [],
        })
        print(f"batch final {batch_index} enviado, total={total}", flush=True)
    except Exception as e:
        import traceback
        print(f"ERRO envio: {e!r}", file=sys.stderr, flush=True)
        traceback.print_exc()
        post_webhook({"started_at": started_at, "error": f"query/envio SAP: {e}"})
        sys.exit(1)

    print(f"enviado (agregado): {total} linhas", flush=True)



if __name__ == "__main__":
    main()
