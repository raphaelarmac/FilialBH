"""
sync_pagamento_pedidos.py — status de pagamento (MIRO) por pedido, via EKKO/EKPO/EKBE.
Env: HANA_DB_HOST, HANA_DB_PORT, HANA_DB_USER, HANA_DB_PASSWORD, HANA_DB_NAME,
     SYNC_WEBHOOK_SECRET, APP_BASE_URL
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
    print("ERRO: HANA_DB_* nao definidos", file=sys.stderr)
    sys.exit(2)
if not WEBHOOK_SECRET:
    print("ERRO: SYNC_WEBHOOK_SECRET nao definido", file=sys.stderr)
    sys.exit(2)

QUERY = """
SELECT
    LTRIM(TRIM(P.EBELN), '0') AS pedido,
    CASE
        WHEN SUM(CASE WHEN H.VGABE = '2' THEN 1 ELSE 0 END) > 0
            THEN 'Pago / Fatura Lançada (MIRO)'
        ELSE 'Não Lançado'
    END AS status_pagamento
FROM EKKO AS K
INNER JOIN EKPO AS P ON K.EBELN = P.EBELN
LEFT JOIN EKBE AS H
       ON P.EBELN = H.EBELN AND P.EBELP = H.EBELP AND H.VGABE = '2'
WHERE (
    K.EKGRP IN ('201', '220', '251')
    OR K.ERNAM IN ('GF.RODRIGUES', 'JO.XAVIER', 'GA.SILVEIRA', 'DS.QUARESMA')
)
GROUP BY P.EBELN
ORDER BY P.EBELN ASC
"""


def post_webhook(payload: dict) -> None:
    url = f"{APP_BASE_URL}/api/public/hooks/sync-pagamento-pedidos"
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
        print(f"query OK: {len(rows)} pedidos", flush=True)
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
        rows = fetch_rows()
    except Exception as e:
        import traceback
        traceback.print_exc()
        post_webhook({"started_at": started_at, "error": f"query SAP: {e}"})
        sys.exit(1)

    dedup: dict[str, dict] = {}
    for r in rows:
        pedido = str(r.get("pedido") or "").strip().lstrip("0")
        if not pedido:
            continue
        status = str(r.get("status_pagamento") or "Não Lançado").strip()
        atual = dedup.get(pedido)
        if atual and "MIRO" in str(atual.get("status_pagamento", "")):
            continue
        dedup[pedido] = {"pedido": pedido, "status_pagamento": status}
    lista = list(dedup.values())

    CHUNK = 2000
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

    print(f"OK: {total} pedidos enviados", flush=True)


if __name__ == "__main__":
    main()
