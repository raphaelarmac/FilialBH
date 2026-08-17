"""
sync_compras_historico.py
=========================
Histórico AMPLIADO de pedidos de compra (PC) do SAP, usado para sugerir
fornecedores/preços/lead time no Painel da Compradora.

Diferente do sync_compras_rcpc.py (fluxo RC→PC recente, filtrado por grupo de
compras), aqui puxamos TODOS os itens de pedido (EKPO/EKKO) de uma janela longa
(padrão 1095 dias = 3 anos), para ter base estatística de preço e prazo.

  POST {APP_BASE_URL}/api/public/hooks/sync-compras-historico
  Header: x-webhook-secret: {SYNC_WEBHOOK_SECRET}
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

DB_HOST = os.environ.get("HANA_DB_HOST") or os.environ.get("SAP_DB_HOST") or ""
_p = os.environ.get("HANA_DB_PORT") or os.environ.get("SAP_DB_PORT")
DB_PORT = int(_p) if _p else 5432
DB_USER = os.environ.get("HANA_DB_USER") or os.environ.get("SAP_DB_USER") or ""
DB_PASSWORD = os.environ.get("HANA_DB_PASSWORD") or os.environ.get("SAP_DB_PASSWORD") or ""
DB_NAME = os.environ.get("HANA_DB_NAME") or os.environ.get("SAP_DB_NAME") or ""

APP_BASE_URL = (os.environ.get("APP_BASE_URL") or "https://gestaofilialbh.lovable.app").rstrip("/")
WEBHOOK_SECRET = os.environ.get("SYNC_WEBHOOK_SECRET") or ""

TOTAL_DIAS = int(os.environ.get("COMPRAS_HIST_DIAS") or 1095)
JANELA = int(os.environ.get("COMPRAS_HIST_JANELA") or 30)
LOTE_ENVIO = int(os.environ.get("COMPRAS_HIST_LOTE") or 2000)

SQL = """
SELECT
  TRIM(LEADING '0' FROM TRIM(P.EBELN)) AS pedido,
  TRIM(CAST(P.EBELP AS VARCHAR)) AS item_pc,
  TRIM(LEADING '0' FROM TRIM(P.MATNR)) AS cod_sap,
  COALESCE(M.MAKTX, P.TXZ01) AS descricao,
  NULLIF(TRIM(LEADING '0' FROM TRIM(COALESCE(K.LIFNR,''))),'') AS fornecedor_cod,
  VEND.NAME1 AS fornecedor_nome,
  K.BEDAT AS data_emissao_pc,
  SCH.data_remessa AS data_remessa_pc,
  P.MENGE AS qtd,
  P.NETPR AS vlr_unit,
  P.NETWR AS vlr_total
FROM EKPO AS P
JOIN EKKO AS K ON P.EBELN = K.EBELN
LEFT JOIN LFA1 AS VEND ON K.LIFNR = VEND.LIFNR
LEFT JOIN LATERAL (
  SELECT MIN(EINDT) AS data_remessa
  FROM EKET
  WHERE EBELN = P.EBELN AND EBELP = P.EBELP
) AS SCH ON TRUE
LEFT JOIN LATERAL (
  SELECT MAX(MAKTX) AS MAKTX
  FROM MAKT
  WHERE MATNR = P.MATNR
    AND SPRAS IN ('P', 'PT')
) AS M ON TRUE
WHERE K.BEDAT >= TO_CHAR(CURRENT_DATE - %(ini)s::int, 'YYYYMMDD')
  AND K.BEDAT <= TO_CHAR(CURRENT_DATE - %(fim)s::int, 'YYYYMMDD')
  AND TRIM(COALESCE(P.LOEKZ,'')) <> 'L'
  AND TRIM(LEADING '0' FROM TRIM(COALESCE(P.MATNR,''))) <> ''
  AND COALESCE(P.NETPR, 0) > 0
ORDER BY P.EBELN ASC
"""


def post_webhook(payload: dict) -> None:
    url = f"{APP_BASE_URL}/api/public/hooks/sync-compras-historico"
    data = json.dumps(payload, default=str).encode("utf-8")
    req = urlrequest.Request(
        url,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "x-webhook-secret": WEBHOOK_SECRET,
        },
    )
    for tentativa in range(3):
        try:
            with urlrequest.urlopen(req, timeout=180) as resp:
                print("webhook:", resp.status, resp.read(300).decode("utf-8", "ignore"))
                return
        except (HTTPError, URLError) as e:
            print(f"falha webhook ({tentativa + 1}/3):", e)
            time.sleep(5 * (tentativa + 1))
    raise RuntimeError("webhook falhou após 3 tentativas")


def conectar():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        connect_timeout=30,
        sslmode=os.environ.get("HANA_DB_SSLMODE") or "prefer",
    )


def buscar_janela(ini: int, fim: int) -> list[dict]:
    ultimo_erro: Exception | None = None
    for tentativa in range(3):
        conn = None
        try:
            conn = conectar()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(SQL, {"ini": ini, "fim": fim})
                return [dict(r) for r in cur.fetchall()]
        except Exception as e:
            ultimo_erro = e
            print(f"erro janela {ini}..{fim} ({tentativa + 1}/3):", e)
            time.sleep(5 * (tentativa + 1))
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
    raise ultimo_erro or RuntimeError("falha desconhecida")


def main() -> int:
    if not WEBHOOK_SECRET:
        print("SYNC_WEBHOOK_SECRET ausente")
        return 1
    started_at = datetime.now(timezone.utc).isoformat()

    vistos: set[str] = set()
    buffer: list[dict] = []
    total = 0
    falhas: list[str] = []

    def flush(final: bool = False) -> None:
        nonlocal buffer
        payload: dict = {"started_at": started_at, "rows": buffer}
        if final:
            payload.update(
                {
                    "final": True,
                    "total": total,
                    "aviso": (
                        f"{len(falhas)} janela(s) falharam e serão refeitas no próximo sync"
                        if falhas
                        else None
                    ),
                }
            )
        post_webhook(payload)
        buffer = []

    ini = TOTAL_DIAS
    while ini > 0:
        fim = max(0, ini - JANELA)
        try:
            rows = buscar_janela(ini, fim)
        except Exception as e:
            falhas.append(str(e))
            ini -= JANELA
            continue

        print(f"janela {ini}..{fim}: {len(rows)} linhas")
        for r in rows:
            pedido = (r.get("pedido") or "").strip()
            item = (r.get("item_pc") or "").strip()
            cod_sap = (r.get("cod_sap") or "").strip()
            if not pedido or not item or not cod_sap:
                continue
            chave = f"{pedido}::{item}"
            if chave in vistos:
                continue
            vistos.add(chave)
            total += 1
            buffer.append(r)
            if len(buffer) >= LOTE_ENVIO:
                flush()
        ini -= JANELA

    flush(final=True)
    print(f"concluído: {total} itens de pedido enviados")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        try:
            post_webhook(
                {
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "error": str(exc),
                }
            )
        except Exception:
            pass
        print("erro fatal:", exc)
        sys.exit(1)
