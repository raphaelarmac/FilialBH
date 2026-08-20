"""
sync_dossie_ativo.py
====================
Roda no GitHub Actions (.github/workflows/sync-dossie-ativo.yml), disparado
pelo app quando o usuário pede o Dossiê de um ativo.

O Cloudflare Worker do app não consegue abrir TLS com a réplica do SAP
(certificado auto-assinado), então a coleta acontece aqui e o resultado é
postado de volta para o app:

  POST {APP_BASE_URL}/api/public/hooks/dossie-ativo-data
  Header: x-webhook-secret: {SYNC_WEBHOOK_SECRET}
  Body:   { "job_id": "...", "ativo": "CM00585", "rows": {...} }
"""
from __future__ import annotations

import json
import os
import sys
import traceback
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError

import psycopg2
import psycopg2.extras

HOST = os.environ.get("HANA_DB_HOST") or os.environ.get("SAP_DB_HOST") or ""
PORT = int(os.environ.get("HANA_DB_PORT") or os.environ.get("SAP_DB_PORT") or 5432)
USER = os.environ.get("HANA_DB_USER") or os.environ.get("SAP_DB_USER") or ""
PASSWORD = os.environ.get("HANA_DB_PASSWORD") or os.environ.get("SAP_DB_PASSWORD") or ""
DBNAME = os.environ.get("HANA_DB_NAME") or os.environ.get("SAP_DB_NAME") or ""

APP_BASE_URL = (os.environ.get("APP_BASE_URL") or "https://gestaofilialbh.lovable.app").rstrip("/")
SECRET = os.environ.get("SYNC_WEBHOOK_SECRET") or ""
ATIVO = (os.environ.get("DOSSIE_ATIVO") or "").strip().upper()
JOB_ID = (os.environ.get("DOSSIE_JOB_ID") or "").strip()

SQL_ORDENS = """
select trim(a.aufnr) aufnr_raw,
       ltrim(trim(a.aufnr),'0') ordem,
       nullif(trim(coalesce(k.auart,'')),'') tipo,
       nullif(trim(coalesce(k.ktext,'')),'') texto,
       k.erdat criada_em
from afih a
left join aufk k on k.aufnr = a.aufnr
where ltrim(trim(coalesce(a.equnr,'')),'0') = %s
order by k.erdat"""

SQL_PEDIDOS = """
select ltrim(trim(n.aufnr),'0') ordem,
  ltrim(trim(p.ebeln),'0') pedido, trim(cast(p.ebelp as varchar)) item,
  ltrim(trim(coalesce(p.matnr,'')),'0') cod_sap, p.txz01 descricao,
  p.menge qtd, p.meins un, p.netpr vlr_unit, p.netwr vlr_total,
  k.bedat data_pc, v.name1 fornecedor,
  nullif(trim(coalesce(p.loekz,'')),'') excluido,
  nullif(ltrim(trim(coalesce(p.banfn,'')),'0'),'') rc
from ekkn n
join ekpo p on p.ebeln = n.ebeln and p.ebelp = n.ebelp
left join ekko k on k.ebeln = p.ebeln
left join lfa1 v on v.lifnr = k.lifnr
where n.aufnr = any(%s::text[])
order by k.bedat, p.ebeln, p.ebelp"""

SQL_RESERVAS = """
select ltrim(trim(r.aufnr),'0') ordem, r.rsnum reserva, r.rspos item,
  ltrim(trim(coalesce(r.matnr,'')),'0') cod_sap, r.sgtxt descricao,
  r.bdmng qtd_reservada, r.enmng qtd_retirada, r.meins un, r.bdter data_necessidade,
  r.gpreis preco_unit,
  case when trim(coalesce(r.xloek,'')) = 'X' then 1 else 0 end estornado
from resb r
where r.aufnr = any(%s::text[])
order by r.aufnr, r.rsnum, r.rspos"""

SQL_CONSUMO = """
select ltrim(trim(m.aufnr),'0') ordem, m.mblnr doc, cast(m.bwart as varchar) mov,
  m.budat_mkpf data, ltrim(trim(coalesce(m.matnr,'')),'0') cod_sap,
  m.sgtxt descricao, m.menge qtd, m.meins un, m.dmbtr valor
from mseg m
where m.aufnr = any(%s::text[])
order by m.budat_mkpf, m.mblnr"""

SQL_MAKT = """
select ltrim(trim(mk.matnr),'0') cod_sap, mk.maktx
from makt mk
where mk.matnr = any(%s::text[]) and mk.spras in ('P','PT')"""


def variantes_sap(valores: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    def add(v: str) -> None:
        if v and v not in seen:
            seen.add(v)
            out.append(v)

    for v in valores:
        s = (v or "").strip()
        if not s:
            continue
        add(s)
        sem = s.lstrip("0")
        if sem:
            add(sem)
            add(sem.rjust(12, "0"))
            add(sem.rjust(18, "0"))
    return out


def variantes_material(valores: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for v in variantes_sap(valores):
        for cand in (v, v.ljust(40, " ")):
            if cand not in seen:
                seen.add(cand)
                out.append(cand)
    return out


def jsonable(v: Any) -> Any:
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", "ignore")
    return v


def rows_of(cur) -> list[dict]:
    return [{k: jsonable(val) for k, val in dict(r).items()} for r in cur.fetchall()]


def post(payload: dict) -> None:
    body = json.dumps(payload).encode("utf-8")
    req = urlrequest.Request(
        f"{APP_BASE_URL}/api/public/hooks/dossie-ativo-data",
        data=body,
        headers={"Content-Type": "application/json", "x-webhook-secret": SECRET},
        method="POST",
    )
    with urlrequest.urlopen(req, timeout=180) as resp:
        print("webhook:", resp.status, resp.read()[:300])


def main() -> int:
    if not all([HOST, USER, PASSWORD, DBNAME, SECRET, ATIVO, JOB_ID]):
        print("ERRO: variáveis obrigatórias ausentes", file=sys.stderr)
        return 2

    conn = psycopg2.connect(
        host=HOST, port=PORT, user=USER, password=PASSWORD, dbname=DBNAME,
        sslmode="require", connect_timeout=30,
    )
    conn.set_session(readonly=True, autocommit=True)
    with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("set statement_timeout = 600000")
        cur.execute(SQL_ORDENS, (ATIVO,))
        ordens = rows_of(cur)

        pedidos: list[dict] = []
        reservas: list[dict] = []
        consumo: list[dict] = []
        makt: list[dict] = []

        if ordens:
            chaves = variantes_sap(
                [str(o.get("aufnr_raw") or "") for o in ordens]
                + [str(o.get("ordem") or "") for o in ordens]
            )
            cur.execute(SQL_PEDIDOS, (chaves,))
            pedidos = rows_of(cur)
            cur.execute(SQL_RESERVAS, (chaves,))
            reservas = rows_of(cur)
            cur.execute(SQL_CONSUMO, (chaves,))
            consumo = rows_of(cur)

            mats = sorted({
                str(r.get("cod_sap") or "").strip()
                for r in (pedidos + reservas + consumo)
                if str(r.get("cod_sap") or "").strip()
            })
            if mats:
                cur.execute(SQL_MAKT, (variantes_material(list(mats)),))
                makt = rows_of(cur)

    print(
        f"ativo={ATIVO} ordens={len(ordens)} pedidos={len(pedidos)} "
        f"reservas={len(reservas)} consumo={len(consumo)} makt={len(makt)}"
    )
    post({
        "job_id": JOB_ID,
        "ativo": ATIVO,
        "rows": {
            "ordens": ordens,
            "pedidos": pedidos,
            "reservas": reservas,
            "consumo": consumo,
            "makt": makt,
        },
    })
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (HTTPError, URLError) as e:
        print("ERRO ao postar no app:", e, file=sys.stderr)
        sys.exit(1)
    except Exception as e:  # noqa: BLE001
        traceback.print_exc()
        try:
            post({"job_id": JOB_ID, "ativo": ATIVO, "error": str(e)[:500]})
        except Exception:
            pass
        sys.exit(1)
