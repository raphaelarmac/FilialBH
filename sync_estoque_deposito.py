"""
sync_estoque_deposito.py
========================
Roda no GitHub Actions (.github/workflows/sync-estoque-deposito.yml).

Traz o estoque COMPLETO do armazém EWM (lgnum = 'OURO') — todos os materiais
com saldo físico, com lote e posição — e não apenas os que têm reserva aberta.
É isso que alimenta a tela "Estoque do depósito".

  POST {APP_BASE_URL}/api/public/hooks/sync-estoque-deposito
  Header: x-webhook-secret: {SYNC_WEBHOOK_SECRET}
  Body:   { "started_at": "...", "rows": [ {...} ] }
"""
from __future__ import annotations

import json
import os
import sys
import time
import traceback
from datetime import date, datetime, timezone
from typing import Any
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError

import psycopg2
import psycopg2.extras

SAP_DB_HOST = os.environ.get("HANA_DB_HOST") or os.environ.get("SAP_DB_HOST") or ""
_sap_port_raw = os.environ.get("HANA_DB_PORT") or os.environ.get("SAP_DB_PORT")
SAP_DB_PORT = int(_sap_port_raw) if _sap_port_raw else 5432
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

ESTOQUE_QUERY = """
WITH ewm_base AS (
    SELECT
        TRIM(LEADING '0' FROM TRIM(mara.matnr)) AS material,
        COALESCE(TRIM(aqua.charg), '') AS lote,
        COALESCE(aqua.lgpla, '') AS posicao,
        COALESCE(aqua.quan, 0) AS quantidade
    FROM "/scwm/aqua" aqua
    LEFT JOIN mara ON aqua.matid = mara.scm_matid_guid16
    WHERE aqua.lgnum = 'OURO' AND aqua.quan > 0
),
por_lote AS (
    SELECT material, lote,
           SUM(quantidade) AS qtd_lote,
           STRING_AGG(DISTINCT posicao, ' | ') AS posicoes
    FROM ewm_base
    WHERE material IS NOT NULL AND material <> ''
    GROUP BY material, lote
),
agrupado AS (
    SELECT material,
           SUM(qtd_lote) AS saldo_estoque,
           STRING_AGG(
               CASE WHEN NULLIF(lote, '') IS NULL THEN posicoes ELSE lote || ' - ' || posicoes END,
               ' / '
           ) AS lotes_com_posicoes
    FROM por_lote
    GROUP BY material
)
SELECT
    a.material AS codigo,
    makt.maktx AS descricao,
    'D005' AS deposito,
    a.saldo_estoque,
    a.lotes_com_posicoes
FROM agrupado a
LEFT JOIN makt ON TRIM(LEADING '0' FROM TRIM(makt.matnr)) = a.material AND makt.spras = 'P'
WHERE a.saldo_estoque > 0
"""


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def jsonable(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.isoformat()
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day, tzinfo=timezone.utc).isoformat()
    return str(v)


def _is_recovery_conflict(err: Exception) -> bool:
    txt = str(err).lower()
    return "conflict with recovery" in txt or "canceling statement due to conflict" in txt


def _fetch_once(query: str) -> list[dict]:
    conn = psycopg2.connect(
        host=SAP_DB_HOST,
        port=SAP_DB_PORT,
        user=SAP_DB_USER,
        password=SAP_DB_PASSWORD,
        dbname=SAP_DB_NAME,
        connect_timeout=20,
        sslmode="require",
        application_name="sync_estoque_deposito",
    )
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query)
            return [dict(r) for r in cur.fetchall()]
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass


def fetch_postgres(query: str, attempts: int = 5) -> list[dict]:
    last: Exception | None = None
    for i in range(attempts):
        try:
            return _fetch_once(query)
        except Exception as e:  # noqa: BLE001
            last = e
            if not _is_recovery_conflict(e) or i == attempts - 1:
                raise
            wait = min(60, 10 * (i + 1))
            print(f"  ! conflito com recovery (tentativa {i + 1}/{attempts}); aguardando {wait}s…", flush=True)
            time.sleep(wait)
    raise last  # type: ignore[misc]


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
            "User-Agent": "sync_estoque_deposito/1.0",
        },
    )
    try:
        with urlrequest.urlopen(req, timeout=300) as resp:
            return {"status": resp.status, "body": resp.read().decode("utf-8") or "{}"}
    except HTTPError as e:
        return {"status": e.code, "body": e.read().decode("utf-8", errors="replace")}
    except URLError as e:
        return {"status": 0, "body": str(e)}


def main() -> int:
    started_at = now_utc_iso()
    print(f"[{started_at}] sync_estoque_deposito — iniciando")

    try:
        rows = fetch_postgres(ESTOQUE_QUERY)
    except Exception as e:  # noqa: BLE001
        msg = f"Falha lendo SAP: {e}"
        print(msg, file=sys.stderr)
        traceback.print_exc()
        resp = post_json("/api/public/hooks/sync-estoque-deposito", {"started_at": started_at, "error": msg})
        print(f"  → status={resp['status']} body={resp['body'][:300]}")
        return 1

    print(f"  Materiais com saldo: {len(rows)}")
    payload_rows = [{k: jsonable(v) for k, v in r.items()} for r in rows]

    # Envia em blocos para não estourar o limite do webhook.
    LOTE = 4000
    for i in range(0, len(payload_rows), LOTE):
        parte = payload_rows[i : i + LOTE]
        resp = post_json(
            "/api/public/hooks/sync-estoque-deposito",
            {
                "started_at": started_at,
                "rows": parte,
                "ultimo_bloco": i + LOTE >= len(payload_rows),
                **(
                    {"todos_codigos": [r.get("codigo") for r in payload_rows]}
                    if i + LOTE >= len(payload_rows)
                    else {}
                ),
            },
        )
        print(f"  bloco {i // LOTE + 1}: status={resp['status']} body={resp['body'][:300]}")
        if not (200 <= resp["status"] < 300):
            return 1
        try:
            body = json.loads(resp["body"] or "{}")
        except json.JSONDecodeError:
            print("ERRO: webhook retornou JSON inválido", file=sys.stderr)
            return 1
        if body.get("ok") is not True:
            print(f"ERRO: webhook não confirmou sucesso: {resp['body'][:500]}", file=sys.stderr)
            return 1
    return 0


def _safe_main() -> int:
    try:
        return main()
    except Exception as e:  # noqa: BLE001
        traceback.print_exc()
        try:
            post_json(
                "/api/public/hooks/sync-estoque-deposito",
                {"started_at": now_utc_iso(), "error": f"Falha inesperada: {e}"},
            )
        except Exception:  # noqa: BLE001
            pass
        return 1


if __name__ == "__main__":
    sys.exit(_safe_main())
