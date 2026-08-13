"""
sync_compras_rcpc.py
====================
Extrai o fluxo de compras RC -> PC do SAP (réplica Postgres/HANA) e envia para
o webhook do app:

  POST {APP_BASE_URL}/api/public/hooks/sync-compras-rcpc
  Header: x-webhook-secret: {SYNC_WEBHOOK_SECRET}
  Body:   { "started_at": "...", "rows": [...], "final": bool, "total": int }

Roda no GitHub Actions (sem limite curto de execução do Worker). A consulta é
fatiada em janelas de dias para não derrubar a conexão da réplica, com retry e
reconexão por janela.

Env necessárias:
  HANA_DB_HOST/PORT/USER/PASSWORD/NAME (ou SAP_DB_*)
  SYNC_WEBHOOK_SECRET, APP_BASE_URL (opcional)
  COMPRAS_DIAS (opcional, padrão 180), COMPRAS_JANELA (opcional, padrão 14)
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

TOTAL_DIAS = int(os.environ.get("COMPRAS_DIAS") or 180)
JANELA = int(os.environ.get("COMPRAS_JANELA") or 14)
LOTE_ENVIO = 800

if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]):
    print("ERRO: HANA_DB_* (ou SAP_DB_*) não definidos", file=sys.stderr)
    sys.exit(2)
if not WEBHOOK_SECRET:
    print("ERRO: SYNC_WEBHOOK_SECRET não definido", file=sys.stderr)
    sys.exit(2)

# Mesma lógica do sync interno (src/server/compras-sync.server.ts).
QUERY = """
SELECT
  COALESCE(NULLIF(TRIM(LEADING '0' FROM TRIM(A.EQUNR)),''), 'ESTOQUE') AS ativo,
  COALESCE(
    NULLIF(TRIM(LEADING '0' FROM TRIM(ACC.AUFNR)),''),
    NULLIF('PC-' || TRIM(LEADING '0' FROM TRIM(COALESCE(P.EBELN,''))), 'PC-'),
    'RC-' || TRIM(LEADING '0' FROM TRIM(E.BANFN))
  ) AS ordem,
  TRIM(LEADING '0' FROM TRIM(E.MATNR)) AS cod_sap,
  COALESCE(M.MAKTX, E.TXZ01) AS descricao,
  COALESCE(P.MENGE, E.MENGE) AS qtd_req,
  NULLIF(TRIM(LEADING '0' FROM TRIM(COALESCE(P.EBELN,''))),'') AS pedido,
  TRIM(COALESCE(CAST(P.EBELP AS VARCHAR), CAST(E.BNFPO AS VARCHAR))) AS num_operacao,
  NULLIF(TRIM(LEADING '0' FROM TRIM(E.BANFN)),'') AS num_rc,
  CAST(E.BNFPO AS VARCHAR) AS item_rc,
  COALESCE(K.ERNAM, E.EKGRP) AS comprador,
  E.ERNAM AS criado_por_sap,
  E.BADAT AS data_requisicao,
  NULLIF(TRIM(LEADING '0' FROM TRIM(COALESCE(K.LIFNR,''))),'') AS fornecedor_cod,
  VEND.NAME1 AS fornecedor_nome,
  K.BEDAT AS data_emissao_pc,
  SCH.data_remessa AS data_remessa_pc,
  COALESCE(P.NETPR, E.PREIS) AS vlr_unit,
  COALESCE(P.NETWR, E.MENGE * E.PREIS) AS vlr_total,
  NULLIF(TRIM(COALESCE(P.KNTTP, E.KNTTP, '')),'') AS knttp,
  CASE
    WHEN COALESCE(TRIM(COALESCE(P.KNTTP, E.KNTTP, '')),'') = '' THEN 'estoque'
    WHEN TRIM(COALESCE(P.KNTTP, E.KNTTP)) = 'K' THEN 'centro_custo'
    ELSE 'consumo'
  END AS destinacao,
  CASE TRIM(COALESCE(P.KNTTP, E.KNTTP, ''))
    WHEN 'K' THEN 'Centro de Custo'
    WHEN 'A' THEN 'Ativo Fixo (Imobilizado)'
    WHEN 'F' THEN 'Ordem (Manutencao/Interna)'
    WHEN 'P' THEN 'Projeto (PEP)'
    WHEN 'Q' THEN 'Projeto (PEP)'
    WHEN 'N' THEN 'Rede'
    WHEN ''  THEN 'Estoque'
    ELSE 'Outro'
  END AS tipo_consumo,
  NULLIF(TRIM(LEADING '0' FROM TRIM(COALESCE(PK.KOSTL, ACC.KOSTL, ''))),'') AS centro_custo,
  CASE
    WHEN COALESCE(H.qtd_recebida, 0) >= P.MENGE AND P.MENGE > 0
      THEN 'recebido'
    WHEN COALESCE(H.qtd_recebida, 0) > 0
      THEN 'entrega_parcial'
    WHEN K.FRGRL = 'X' THEN 'aguardando_aprovacao'
    WHEN P.EBELN IS NOT NULL AND COALESCE(TRIM(P.LOEKZ),'') <> 'L' THEN 'pedido_emitido'
    WHEN TRIM(COALESCE(P.LOEKZ,'')) = 'L' THEN 'pedido_cancelado'
    WHEN TRIM(COALESCE(E.LOEKZ,'')) = 'X' THEN 'rc_excluida'
    WHEN E.BANFN IS NOT NULL AND COALESCE(TRIM(P.EBELN),'') = '' THEN 'aguardando_pedido'
    ELSE 'verificar'
  END AS status_processo
FROM EBAN AS E
LEFT JOIN EKPO AS P ON E.BANFN = P.BANFN AND E.BNFPO = P.BNFPO
LEFT JOIN EKKO AS K ON P.EBELN = K.EBELN
LEFT JOIN LATERAL (
  SELECT MAX(AUFNR) AS AUFNR, MAX(KOSTL) AS KOSTL
  FROM EBKN
  WHERE BANFN = E.BANFN AND BNFPO = E.BNFPO
) AS ACC ON TRUE
LEFT JOIN LATERAL (
  SELECT MAX(KOSTL) AS KOSTL
  FROM EKKN
  WHERE EBELN = P.EBELN AND EBELP = P.EBELP
) AS PK ON TRUE
LEFT JOIN AFIH AS A ON ACC.AUFNR = A.AUFNR
LEFT JOIN LFA1 AS VEND ON K.LIFNR = VEND.LIFNR
LEFT JOIN LATERAL (
  SELECT SUM(MENGE) AS qtd_recebida
  FROM EKBE
  WHERE EBELN = P.EBELN AND EBELP = P.EBELP AND VGABE = '1'
) AS H ON TRUE
LEFT JOIN LATERAL (
  SELECT MIN(EINDT) AS data_remessa
  FROM EKET
  WHERE EBELN = P.EBELN AND EBELP = P.EBELP
) AS SCH ON TRUE
LEFT JOIN LATERAL (
  SELECT MAX(MAKTX) AS MAKTX
  FROM MAKT
  WHERE MATNR = E.MATNR
    AND SPRAS IN ('P', 'PT')
) AS M ON TRUE
WHERE (
    E.EKGRP IN ('201', '220', '251')
    OR K.EKGRP IN ('201', '220', '251')
    OR K.ERNAM IN ('GF.RODRIGUES', 'JO.XAVIER', 'GA.SILVEIRA', 'DS.QUARESMA')
  )
  AND E.BADAT >= TO_CHAR(CURRENT_DATE - %(ini)s::int, 'YYYYMMDD')
  AND E.BADAT <= TO_CHAR(CURRENT_DATE - %(fim)s::int, 'YYYYMMDD')
  AND TRIM(LEADING '0' FROM TRIM(E.MATNR)) <> ''
ORDER BY E.BANFN ASC
"""


def post_webhook(payload: dict) -> None:
    url = f"{APP_BASE_URL}/api/public/hooks/sync-compras-rcpc"
    data = json.dumps(payload, default=str).encode("utf-8")
    req = urlrequest.Request(
        url,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "x-webhook-secret": WEBHOOK_SECRET,
            "User-Agent": "Mozilla/5.0 (compatible; ArmacSync/1.0)",
        },
    )
    last = None
    for tentativa in range(3):
        try:
            with urlrequest.urlopen(req, timeout=300) as resp:
                print(f"webhook {resp.status}: {resp.read().decode('utf-8', 'ignore')[:300]}")
                return
        except HTTPError as e:
            last = f"HTTP {e.code}: {e.read().decode('utf-8', 'ignore')[:300]}"
        except URLError as e:
            last = f"URL error: {e.reason}"
        time.sleep(5 * (tentativa + 1))
    raise RuntimeError(f"webhook falhou: {last}")


def conectar():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        connect_timeout=30,
        sslmode=os.environ.get("HANA_DB_SSLMODE") or "prefer",
        options="-c statement_timeout=600000",
    )


def buscar_janela(ini: int, fim: int) -> list[dict]:
    ultimo = None
    for tentativa in range(1, 5):
        conn = None
        try:
            conn = conectar()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(QUERY, {"ini": ini, "fim": fim})
                return [dict(r) for r in cur.fetchall()]
        except Exception as e:  # noqa: BLE001
            ultimo = e
            print(f"  janela {ini}..{fim} tentativa {tentativa} falhou: {e}", file=sys.stderr)
            time.sleep(min(60, 5 * tentativa * tentativa))
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass
    raise RuntimeError(f"janela {ini}..{fim} falhou: {ultimo}")


def main() -> None:
    started_at = datetime.now(timezone.utc).isoformat()
    vistos: set[str] = set()
    buffer: list[dict] = []
    total = 0
    com_pedido = 0
    falhas: list[str] = []
    sucessos = 0

    def flush(final: bool = False) -> None:
        nonlocal buffer
        if not buffer and not final:
            return
        payload = {"started_at": started_at, "rows": buffer}
        if final:
            payload.update(
                {
                    "final": True,
                    "total": total,
                    "com_pedido": com_pedido,
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
            sucessos += 1
        except Exception as e:  # noqa: BLE001
            falhas.append(str(e))
            ini -= JANELA
            continue

        print(f"janela {ini}..{fim}: {len(rows)} linhas")
        for r in rows:
            num_rc = (r.get("num_rc") or "").strip()
            item_rc = (r.get("item_rc") or "").strip()
            cod_sap = (r.get("cod_sap") or "").strip()
            if not num_rc or not item_rc or not cod_sap:
                continue
            chave = f"{num_rc}::{item_rc}"
            if chave in vistos:
                continue
            vistos.add(chave)
            total += 1
            if (r.get("pedido") or "").strip():
                com_pedido += 1
            buffer.append(r)
            if len(buffer) >= LOTE_ENVIO:
                flush()

        ini -= JANELA

    if sucessos == 0:
        erro = falhas[0] if falhas else "sem detalhe"
        post_webhook({"started_at": started_at, "error": f"todas as janelas falharam: {erro}"})
        print(f"ERRO: {erro}", file=sys.stderr)
        sys.exit(1)

    flush(final=True)
    print(f"OK: {total} itens ({com_pedido} com pedido), {len(falhas)} janela(s) com falha")


if __name__ == "__main__":
    main()
