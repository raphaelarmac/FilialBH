"""
sync_migo.py — MIGO (recebimento físico) por pedido, do MySQL de requisições de compras.
Env: MIGO_DB_HOST, MIGO_DB_PORT, MIGO_DB_USER, MIGO_DB_PASSWORD, MIGO_DB_NAME,
     SYNC_WEBHOOK_SECRET, APP_BASE_URL
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, date, timezone
from decimal import Decimal
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError

import pymysql
import pymysql.cursors

DB_HOST = os.environ.get("MIGO_DB_HOST") or ""
DB_PORT = int(os.environ.get("MIGO_DB_PORT") or 3306)
DB_USER = os.environ.get("MIGO_DB_USER") or ""
DB_PASSWORD = os.environ.get("MIGO_DB_PASSWORD") or ""
DB_NAME = os.environ.get("MIGO_DB_NAME") or ""

APP_BASE_URL = (os.environ.get("APP_BASE_URL") or "https://gestaofilialbh.lovable.app").rstrip("/")
WEBHOOK_SECRET = os.environ.get("SYNC_WEBHOOK_SECRET") or ""

if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]):
    print("ERRO: MIGO_DB_HOST / MIGO_DB_USER / MIGO_DB_PASSWORD / MIGO_DB_NAME nao definidos", file=sys.stderr)
    sys.exit(2)
if not WEBHOOK_SECRET:
    print("ERRO: SYNC_WEBHOOK_SECRET nao definido", file=sys.stderr)
    sys.exit(2)

QUERY = """
WITH main AS (
    SELECT
        TRIM(LEADING '0' FROM COALESCE(NULLIF(er.requisicao_de_compra_os, ''), rc.numero_pedido_gerado, '')) AS requisicao,
        er.pedido,
        er.data_recebimento AS data_do_recebimento_fisico
    FROM evidencia_recebimentos er
    LEFT JOIN requisicoes_de_compra rc ON er.requisicao_de_compra_id = rc.id
)
SELECT
    m.pedido,
    m.requisicao,
    m.data_do_recebimento_fisico,
    CASE
        WHEN m.data_do_recebimento_fisico IS NOT NULL THEN 'MIGO FEITA'
        ELSE 'MIGO PENDENTE'
    END AS status_migo
FROM main m
ORDER BY status_migo ASC, m.data_do_recebimento_fisico DESC
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
    url = f"{APP_BASE_URL}/api/public/hooks/sync-migo"
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
            print(f"webhook {resp.status}: {resp.read()[:160].decode('utf-8', 'ignore')}", flush=True)
    except HTTPError as e:
        print(f"HTTP {e.code}: {e.read()[:400].decode('utf-8', 'ignore')}", file=sys.stderr, flush=True)
        raise
    except URLError as e:
        print(f"URLError: {e}", file=sys.stderr, flush=True)
        raise


def fetch_rows() -> list[dict]:
    print(f"conectando no MySQL {DB_HOST}:{DB_PORT}/{DB_NAME}...", flush=True)
    conn = pymysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD,
        database=DB_NAME, connect_timeout=30, read_timeout=600,
        cursorclass=pymysql.cursors.DictCursor, charset="utf8mb4",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(QUERY)
            rows = cur.fetchall()
        print(f"query OK: {len(rows)} linhas", flush=True)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    seen: dict[str, dict] = {}
    for r in rows:
        pedido = str(r.get("pedido") or "").strip()
        if not pedido or pedido in seen:
            continue
        seen[pedido] = {k: to_jsonable(v) for k, v in r.items()}
    return list(seen.values())


def main() -> None:
    started_at = datetime.now(timezone.utc).isoformat()
    try:
        rows = fetch_rows()
    except Exception as e:
        import traceback
        traceback.print_exc()
        post_webhook({"started_at": started_at, "error": f"query MySQL: {e}"})
        sys.exit(1)

    CHUNK = 2000
    total = 0
    try:
        if not rows:
            post_webhook({"started_at": started_at, "rows": [], "is_last": True})
        for i in range(0, len(rows), CHUNK):
            buf = rows[i:i + CHUNK]
            post_webhook({
                "started_at": started_at,
                "rows": buf,
                "batch_index": i // CHUNK,
                "is_last": i + CHUNK >= len(rows),
            })
            total += len(buf)
            print(f"enviado {total}/{len(rows)}", flush=True)
    except Exception as e:
        import traceback
        traceback.print_exc()
        post_webhook({"started_at": started_at, "error": f"envio: {e}"})
        sys.exit(1)

    print(f"OK: {total} pedidos enviados", flush=True)


if __name__ == "__main__":
    main()
