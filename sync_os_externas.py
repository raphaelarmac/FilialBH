"""
sync_os_externas.py
===================
Lê no datalake SAP as OSs de manutenção dos responsáveis externos
(Heitor 22702 e Paulo Alvarenga 22044) — query FINAL v6 de 12/08/2026 —
e envia 1 linha por ORDEM (com as operações aninhadas) ao webhook do app:

  POST {APP_BASE_URL}/api/public/hooks/sync-os-externas
  Header: x-webhook-secret: {SYNC_WEBHOOK_SECRET}

Env: HANA_DB_HOST, HANA_DB_PORT, HANA_DB_USER, HANA_DB_PASSWORD, HANA_DB_NAME,
     SYNC_WEBHOOK_SECRET, APP_BASE_URL
"""
from __future__ import annotations

import json
import os
# Remove espacos/quebras de linha acidentais colados nos secrets do GitHub.
for _k, _v in list(os.environ.items()):
    if isinstance(_v, str) and _v != _v.strip():
        os.environ[_k] = _v.strip()

import sys
import time
from datetime import datetime, date, timezone
from decimal import Decimal
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
    print("ERRO: HANA_DB_* nao definidos", file=sys.stderr)
    sys.exit(2)
if not WEBHOOK_SECRET:
    print("ERRO: SYNC_WEBHOOK_SECRET nao definido", file=sys.stderr)
    sys.exit(2)

# =====================================================================
# OS de Manutenção - Heitor (22702) e Paulo Alvarenga (22044)
# Datalake SAP (sintaxe PostgreSQL) - FINAL v6 - 12/08/2026
# =====================================================================
QUERY = """
WITH base_os AS (
  SELECT
    AUFK.AUFNR,
    AUFK.OBJNR,
    AUFK.KTEXT      AS descricao_os,
    AUFK.ERDAT      AS data_criacao,
    AUFK.ERNAM      AS autor_os,
    AUFK.IDAT1      AS data_liberacao,
    AUFK.IDAT2      AS data_enc_tecnico,
    AUFK.IDAT3      AS data_enc_comercial,
    AFIH.EQUNR      AS ativo,
    CRHD_CAB.ARBPL  AS centro_trabalho_cabecalho,
    AFIH.maintordpersonresponsible AS cod_responsavel,
    RESP.ename      AS responsavel,
    AFIH.ILART      AS tipo_manutencao_real,
    CASE
      WHEN AFIH.ILART LIKE '%PRP%' THEN 'Preparação'
      WHEN AFIH.ILART LIKE '%OFI%' THEN 'Oficina'
      ELSE 'Outro'
    END AS tipo_manutencao,
    CASE
      WHEN AUFK.LOEKZ = 'X' THEN 'Marcada p/ eliminação'
      WHEN AUFK.PHAS3 = 'X' THEN 'Encerrada'
      WHEN AUFK.PHAS2 = 'X' THEN 'Encerrada tecnicamente'
      WHEN AUFK.PHAS1 = 'X' THEN 'Liberada'
      WHEN AUFK.PHAS0 = 'X' THEN 'Aberta'
      ELSE 'Indefinido'
    END AS situacao_os
  FROM AUFK
  INNER JOIN AFIH
          ON AUFK.AUFNR = AFIH.AUFNR
  LEFT JOIN CRHD CRHD_CAB
         ON AFIH.GEWRK = CRHD_CAB.OBJID
  LEFT JOIN (
        SELECT pernr, MAX(ename) AS ename
        FROM PA0001
        GROUP BY pernr
       ) RESP
         ON AFIH.maintordpersonresponsible::text = RESP.pernr::text
  WHERE AUFK.ERDAT >= '2024-01-01'
    AND TRIM(LEADING '0' FROM TRIM(AFIH.maintordpersonresponsible::text))
        IN ('22702', '22044')
),
dados_planejamento AS (
  SELECT
    OS.AUFNR,
    MAX(AFKO.GSTRP) AS data_inicio_programada,
    MAX(AFKO.GLTRP) AS data_fim_programada,
    MAX(AFKO.GSTRI) AS data_inicio_real,
    MAX(AFKO.GETRI) AS data_fim_real,
    MAX(AFKO.AUFPL) AS aufpl
  FROM base_os OS
  LEFT JOIN AFKO ON OS.AUFNR = AFKO.AUFNR
  GROUP BY OS.AUFNR
),
operacoes AS (
  SELECT
    D.AUFNR,
    AFVC.VORNR     AS num_operacao,
    AFVC.LTXA1     AS descricao_operacao,
    CRHD_OP.ARBPL  AS centro_trabalho_operacao,
    AFVV.ARBEI     AS tempo_previsto_h
  FROM dados_planejamento D
  LEFT JOIN AFVC
         ON D.aufpl = AFVC.AUFPL
  LEFT JOIN AFVV
         ON AFVC.AUFPL = AFVV.AUFPL
        AND AFVC.APLZL = AFVV.APLZL
  LEFT JOIN CRHD CRHD_OP
         ON AFVC.ARBID = CRHD_OP.OBJID
),
status_ordem AS (
  SELECT
    TRIM(BOTH FROM J.OBJNR) AS objnr,
    STRING_AGG(DISTINCT SU.TXT04, ' ' ORDER BY SU.TXT04)   AS status_usuario,
    STRING_AGG(DISTINCT SU.TXT30, ' | ' ORDER BY SU.TXT30) AS status_usuario_desc,
    STRING_AGG(DISTINCT ST.TXT04, ' ' ORDER BY ST.TXT04)   AS status_sistema
  FROM JEST J
  LEFT JOIN TJ02T ST
         ON J.STAT = ST.ISTAT
        AND ST.SPRAS = 'P'
  LEFT JOIN JSTO JS
         ON TRIM(BOTH FROM JS.OBJNR) = TRIM(BOTH FROM J.OBJNR)
  LEFT JOIN TJ30T SU
         ON J.STAT = SU.ESTAT
        AND TRIM(BOTH FROM SU.STSMA) = TRIM(BOTH FROM JS.STSMA)
        AND SU.SPRAS = 'P'
  WHERE COALESCE(J.INACT::text, '') <> 'X'
    AND TRIM(BOTH FROM J.OBJNR) IN (SELECT TRIM(BOTH FROM OBJNR) FROM base_os)
  GROUP BY TRIM(BOTH FROM J.OBJNR)
)
SELECT
  OS.ativo,
  TRIM(LEADING '0' FROM OS.AUFNR) AS ordem,
  OS.situacao_os,
  SO.status_usuario               AS status_os_usuario,
  SO.status_usuario_desc          AS status_os_usuario_descricao,
  SO.status_sistema               AS status_os_sistema,
  OS.tipo_manutencao,
  OS.tipo_manutencao_real,
  OS.descricao_os,
  OS.centro_trabalho_cabecalho    AS centro_trabalho_responsavel,
  OS.cod_responsavel,
  OS.responsavel,
  DP.data_inicio_programada,
  DP.data_fim_programada,
  DP.data_inicio_real,
  DP.data_fim_real,
  OS.data_liberacao,
  OS.data_enc_tecnico,
  OS.data_enc_comercial,
  OS.data_criacao,
  OS.autor_os,
  OP.num_operacao,
  OP.descricao_operacao,
  OP.centro_trabalho_operacao,
  OP.tempo_previsto_h
FROM base_os OS
LEFT JOIN dados_planejamento DP ON OS.AUFNR = DP.AUFNR
LEFT JOIN operacoes OP          ON OS.AUFNR = OP.AUFNR
LEFT JOIN status_ordem SO       ON TRIM(BOTH FROM OS.OBJNR) = SO.objnr
ORDER BY OS.ativo, OS.AUFNR DESC, OP.num_operacao
"""


def to_jsonable(v):
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", "ignore")
    return v


def post_webhook(payload: dict) -> None:
    url = f"{APP_BASE_URL}/api/public/hooks/sync-os-externas"
    data = json.dumps(payload, default=str).encode("utf-8")
    req = urlrequest.Request(
        url, data=data, method="POST",
        headers={
            "Content-Type": "application/json",
            "x-webhook-secret": WEBHOOK_SECRET,
            "User-Agent": "Mozilla/5.0 (compatible; ArmacSync/1.0)",
        },
    )
    try:
        with urlrequest.urlopen(req, timeout=180) as resp:
            print(f"webhook {resp.status}: {resp.read()[:120].decode('utf-8', 'ignore')}", flush=True)
    except HTTPError as e:
        print(f"HTTP {e.code}: {e.read()[:400].decode('utf-8', 'ignore')}", file=sys.stderr, flush=True)
        raise
    except URLError as e:
        print(f"URLError: {e}", file=sys.stderr, flush=True)
        raise


def _is_recovery_conflict(err: Exception) -> bool:
    txt = str(err).lower()
    return "conflict with recovery" in txt or "canceling statement due to conflict" in txt


def _fetch_once() -> list[dict]:
    print(f"conectando no SAP em {SAP_DB_HOST}:{SAP_DB_PORT}/{SAP_DB_NAME}...", flush=True)
    conn = psycopg2.connect(
        host=SAP_DB_HOST, port=SAP_DB_PORT, user=SAP_DB_USER,
        password=SAP_DB_PASSWORD, dbname=SAP_DB_NAME, connect_timeout=30,
    )
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(QUERY)
            rows = cur.fetchall()
        print(f"query OK: {len(rows)} linhas (ordem x operacao)", flush=True)
        return [dict(r) for r in rows]
    finally:
        conn.close()


def fetch_rows() -> list[dict]:
    last: Exception | None = None
    for tentativa in range(1, 4):
        try:
            return _fetch_once()
        except Exception as e:
            last = e
            if _is_recovery_conflict(e) and tentativa < 3:
                espera = 30 * tentativa
                print(f"conflito de replica; retry em {espera}s...", flush=True)
                time.sleep(espera)
                continue
            raise
    raise last  # type: ignore[misc]


def main() -> None:
    started_at = datetime.now(timezone.utc).isoformat()
    try:
        raw = fetch_rows()
    except Exception as e:
        import traceback
        traceback.print_exc()
        post_webhook({"started_at": started_at, "error": f"query SAP: {e}"})
        sys.exit(1)

    # Agrupa 1 linha por ordem, com as operacoes aninhadas.
    ordens: dict[str, dict] = {}
    for r in raw:
        ordem = str(r.get("ordem") or "").strip().lstrip("0")
        if not ordem:
            continue
        o = ordens.get(ordem)
        if not o:
            o = {
                "ordem": ordem,
                "ativo": to_jsonable(r.get("ativo")),
                "situacao_os": to_jsonable(r.get("situacao_os")),
                "status_usuario": to_jsonable(r.get("status_os_usuario")),
                "status_usuario_desc": to_jsonable(r.get("status_os_usuario_descricao")),
                "status_sistema": to_jsonable(r.get("status_os_sistema")),
                "tipo_manutencao": to_jsonable(r.get("tipo_manutencao")),
                "tipo_manutencao_real": to_jsonable(r.get("tipo_manutencao_real")),
                "descricao_os": to_jsonable(r.get("descricao_os")),
                "centro_trabalho": to_jsonable(r.get("centro_trabalho_responsavel")),
                "cod_responsavel": str(r.get("cod_responsavel") or "").strip().lstrip("0") or None,
                "responsavel": to_jsonable(r.get("responsavel")),
                "autor_os": to_jsonable(r.get("autor_os")),
                "data_criacao": to_jsonable(r.get("data_criacao")),
                "data_liberacao": to_jsonable(r.get("data_liberacao")),
                "data_inicio_programada": to_jsonable(r.get("data_inicio_programada")),
                "data_fim_programada": to_jsonable(r.get("data_fim_programada")),
                "data_inicio_real": to_jsonable(r.get("data_inicio_real")),
                "data_fim_real": to_jsonable(r.get("data_fim_real")),
                "data_enc_tecnico": to_jsonable(r.get("data_enc_tecnico")),
                "data_enc_comercial": to_jsonable(r.get("data_enc_comercial")),
                "operacoes": [],
            }
            ordens[ordem] = o
        num_op = r.get("num_operacao")
        if num_op is not None and str(num_op).strip():
            op = {
                "num_operacao": str(num_op).strip(),
                "descricao_operacao": to_jsonable(r.get("descricao_operacao")),
                "centro_trabalho_operacao": to_jsonable(r.get("centro_trabalho_operacao")),
                "tempo_previsto_h": to_jsonable(r.get("tempo_previsto_h")),
            }
            if not any(x["num_operacao"] == op["num_operacao"] for x in o["operacoes"]):
                o["operacoes"].append(op)

    lista = list(ordens.values())
    print(f"{len(lista)} ordens agrupadas", flush=True)

    CHUNK = 300
    total = 0
    try:
        if not lista:
            post_webhook({"started_at": started_at, "rows": [], "is_last": True})
        for i in range(0, len(lista), CHUNK):
            buf = lista[i:i + CHUNK]
            post_webhook({
                "started_at": started_at,
                "rows": buf,
                "batch_index": i // CHUNK,
                "is_last": i + CHUNK >= len(lista),
            })
            total += len(buf)
            print(f"enviado {total}/{len(lista)}", flush=True)
    except Exception as e:
        import traceback
        traceback.print_exc()
        post_webhook({"started_at": started_at, "error": f"envio: {e}"})
        sys.exit(1)

    print(f"OK: {total} ordens enviadas", flush=True)


if __name__ == "__main__":
    main()
