"""
sync_migo.py
============
Lê o banco MySQL de requisições de compras (RDS réplica readonly) e envia o
status de MIGO (recebimento físico) por pedido para o webhook do app:

  POST {APP_BASE_URL}/api/public/hooks/sync-migo
  Header: x-webhook-secret: {SYNC_WEBHOOK_SECRET}
  Body:   { "started_at": "...", "rows": [ {pedido, requisicao, data_do_recebimento_fisico, status_migo}, ... ] }

Env necessárias:
  MIGO_DB_HOST, MIGO_DB_PORT (3306), MIGO_DB_USER, MIGO_DB_PASSWORD, MIGO_DB_NAME
  SYNC_WEBHOOK_SECRET, APP_BASE_URL (opcional)
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

import pymysql
import pymysql.cursors

DB_HOST = os.environ.get("MIGO_DB_HOST") or os.environ.get("ARMAC_DB_HOST") or ""
DB_PORT = int(os.environ.get("MIGO_DB_PORT") or os.environ.get("ARMAC_DB_PORT") or 3306)
DB_USER = os.environ.get("MIGO_DB_USER") or os.environ.get("ARMAC_DB_USER") or ""
DB_PASSWORD = os.environ.get("MIGO_DB_PASSWORD") or os.environ.get("ARMAC_DB_PASSWORD") or ""
DB_NAME = os.environ.get("MIGO_DB_NAME") or "requisicoes_compras"

APP_BASE_URL = (os.environ.get("APP_BASE_URL") or "https://gestaofilialbh.lovable.app").rstrip("/")
WEBHOOK_SECRET = os.environ.get("SYNC_WEBHOOK_SECRET") or ""

if not DB_HOST:
    print("ERRO: defina MIGO_DB_HOST (ou ARMAC_DB_HOST) nos secrets do GitHub", file=sys.stderr)
    sys.exit(2)

if not DB_USER or not DB_PASSWORD:
    print("ERRO: defina MIGO_DB_USER/MIGO_DB_PASSWORD (ou ARMAC_DB_USER/ARMAC_DB_PASSWORD)", file=sys.stderr)
    sys.exit(2)


if not WEBHOOK_SECRET:
    print("ERRO: SYNC_WEBHOOK_SECRET não definido", file=sys.stderr)
    sys.exit(2)

# Mesma lógica da query do Power BI (Table.Sort por status + Distinct por pedido):
# ordenamos MIGO FEITA antes de MIGO PENDENTE e mantemos 1 linha por pedido.
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


def post_webhook(payload: dict, tentativas: int = 4) -> None:
    for i in range(tentativas):
        try:
            _post_webhook_once(payload)
            return
        except Exception as e:
            if i == tentativas - 1:
                raise
            espera = 5 * (i + 1)
            print(f"retry envio em {espera}s ({e})", file=sys.stderr, flush=True)
            time.sleep(espera)


def _post_webhook_once(payload: dict) -> None:
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

    # Distinct por pedido mantendo a primeira ocorrência (MIGO FEITA vem antes)
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

    CHUNK = 500
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
