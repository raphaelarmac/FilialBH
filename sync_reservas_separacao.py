"""
sync_reservas_separacao.py
==========================
Roda no GitHub Actions (.github/workflows/sync-reservas-separacao.yml).
Lê o Postgres réplica do SAP (HANA_DB_*) executando a query de reservas de
separação (EWM) e envia o payload flat para o webhook do app:

  POST {APP_BASE_URL}/api/public/hooks/sync-reservas-separacao
  Header: x-webhook-secret: {SYNC_WEBHOOK_SECRET}
  Body:   { "started_at": "...", "rows": [ {...}, {...} ] }
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
import traceback
from datetime import date, datetime, timezone
from typing import Any
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError

import psycopg2
import psycopg2.extras

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

if not all([SAP_DB_HOST, SAP_DB_USER, SAP_DB_PASSWORD, SAP_DB_NAME]):
    print("ERRO: HANA_DB_* (ou SAP_DB_*) não definidos", file=sys.stderr)
    sys.exit(2)
if not WEBHOOK_SECRET:
    print("ERRO: SYNC_WEBHOOK_SECRET não definido", file=sys.stderr)
    sys.exit(2)

# ---------------------------------------------------------------------------
# QUERY oficial — Reservas de Separação (EWM) com saldo estoque
# ---------------------------------------------------------------------------
RESERVAS_SEPARACAO_QUERY = """
WITH
base_resb AS (
    SELECT
        rsnum, rspos, matnr, bdmng, enmng, werks, lgort, bwart, sgtxt,
        wempf, ablad, objnr, xloek, kzear, postp, xwaok,
        (LPAD(TRIM(rsnum)::text, 10, '0') || LPAD(TRIM(rspos)::text, 4, '0')) AS chave_reserva
    FROM resb
    WHERE lgort IN ('D005', 'D090')
      AND sgtxt <> 'Item não estocável'
      AND (postp = 'L' OR postp IS NULL OR postp = '')
      AND xloek <> 'X'
      AND bwart IN ('201', '261')
),
materiais_base AS (
    SELECT DISTINCT TRIM(LEADING '0' FROM TRIM(matnr)) AS material_limpo
    FROM base_resb
),
tipos_ordem AS (
    SELECT auart, txt FROM t003p WHERE spras = 'P'
),
aprovacao_reservas AS (
    SELECT DISTINCT ON (TRIM(z.chave::text))
        TRIM(z.chave::text) AS chave,
        NULLIF(TRIM(z.acao::text), '') AS status,
        NULLIF(TRIM(z.data::text), '') AS data,
        NULLIF(TRIM(z.hora::text), '') AS hora
    FROM ztwf_log_wf z
    INNER JOIN base_resb b ON TRIM(z.chave::text) = b.chave_reserva
    ORDER BY TRIM(z.chave::text), z.data DESC NULLS LAST, z.hora DESC NULLS LAST
),
ewm_base AS (
    SELECT
        TRIM(LEADING '0' FROM TRIM(mara.matnr)) AS material,
        COALESCE(TRIM(aqua.charg), '') AS lote,
        COALESCE(aqua.lgpla, '') AS posicao,
        COALESCE(aqua.quan, 0) AS quantidade
    FROM "/scwm/aqua" aqua
    LEFT JOIN mara ON aqua.matid = mara.scm_matid_guid16
    WHERE aqua.lgnum = 'OURO' AND aqua.quan > 0
),
lotes_ewm_agrupados AS (
    SELECT
        material,
        SUM(qtd_lote) AS saldo_estoque,
        STRING_AGG(
            CASE WHEN NULLIF(lote, '') IS NULL THEN posicoes ELSE lote || ' - ' || posicoes END,
            ' / '
        ) AS lotes_com_posicoes
    FROM (
        SELECT
            material,
            lote,
            SUM(quantidade) AS qtd_lote,
            STRING_AGG(DISTINCT posicao, ' | ') AS posicoes
        FROM ewm_base
        WHERE material IN (SELECT material_limpo FROM materiais_base)
        GROUP BY material, lote
    ) sub
    GROUP BY material
),
usuarios_sap AS (
    SELECT DISTINCT TRIM(usr21.bname) AS bname_chave, adrp.name_text AS nome_usuario
    FROM usr21
    LEFT JOIN adrp ON TRIM(usr21.persnumber) = TRIM(adrp.persnumber)
)
SELECT DISTINCT
    COALESCE(CAST(resb.rsnum AS INT), 0)::text AS numero_reserva,
    LTRIM(TRIM(resb.matnr), '0') AS codigo,
    makt.maktx AS descr_material,
    resb.bdmng AS qtd_reserva,
    COALESCE(resb.enmng, 0) AS qtd_retirada,
    resb.kzear AS item_baixado,
    COALESCE(CAST(resb.rspos AS INT), 0)::text AS numero_item,
    resb.chave_reserva AS chave,
    COALESCE(CAST(aufk.aufnr AS INT), 0)::text AS numero_ordem,
    CASE WHEN rsadd.creadat IS NOT NULL THEN rsadd.creadat ELSE rkpf.rsdat END AS data,
    resb.werks AS centro,
    resb.lgort AS deposito,
    CASE
        WHEN resb.bwart = '201' THEN 'Reserva Manual'
        WHEN resb.bwart = '261' THEN 'Ordem'
        ELSE ''
    END AS descricao_movimentacao,
    COALESCE(tipos_ordem.txt, '') AS tipo_ordem,
    COALESCE(csks.kostl, '') AS centro_custo,
    CASE
        WHEN LENGTH(aufk.kostl) = 10 THEN cepct.ltext
        WHEN LENGTH(aufk.kostl) = 7 THEN anlh.anlhtxt
    END AS descricao_ativo,
    rkpf.usnam AS criador_reserva,
    u_criador_res.nome_usuario AS nome_criador_reserva,
    rkpf.lastchangedbyuser AS ult_mod_reserva,
    u_mod_res.nome_usuario AS nome_ult_mod_reserva,
    CASE
        WHEN rkpf.lastchangedatetime = '0.0000000' THEN ''
        ELSE SUBSTRING(CAST(rkpf.lastchangedatetime AS TEXT), 7, 2) || '-' ||
             SUBSTRING(CAST(rkpf.lastchangedatetime AS TEXT), 5, 2) || '-' ||
             SUBSTRING(CAST(rkpf.lastchangedatetime AS TEXT), 0, 5)
    END AS data_ult_mod_reserva,
    aufk.ernam AS criador_ordem,
    aufk.aenam AS ult_mod_ordem,
    ar.status AS aprovacao_reserva,
    ar.data AS data_aprovacao,
    ar.hora AS hora_aprovacao,
    resb.sgtxt AS texto,
    resb.wempf AS recebedor,
    resb.ablad AS ponto_descarga,
    resb.xwaok AS status_aprovacao_sap,
    lips.vbeln AS remessa,
    resb.objnr AS chavejest,
    resb.xloek AS cancelado,
    resb.kzear AS finalizado,

    ewm.lotes_com_posicoes,
    COALESCE(ewm.saldo_estoque, 0) AS saldo_estoque,

    pm.n_equipamento, pm.cod_tipo_ordem, pm.tipo_ordem AS pm_tipo_ordem,
    pm.texto_breve AS pm_texto_breve, pm.cod_prioridade, pm.cod_centro_trabalho,
    pm.prioridade, pm.cod_status_usuario, pm.status_usuario, pm.local_instalacao,
    pm.cod_grupo_plan, pm.grupo_plan, pm.cod_tipo_atividade, pm.tipo_atividade,
    pm.n_nota, pm.cod_responsavel, pm.responsavel, pm.cod_ultimo_modificador,
    pm.email_ultimo_modificador,
    CASE WHEN pm.data_ultima_modificacao::text = '0000-00-00' THEN NULL ELSE pm.data_ultima_modificacao END AS data_ultima_modificacao,
    pm.hora_ultima_modificacao,
    CASE WHEN pm.datetime_ultima_modificacao::text = '0000-00-00' THEN NULL ELSE pm.datetime_ultima_modificacao END AS datetime_ultima_modificacao,
    pm.n_plano_manutencao, pm.n_solicitacao_manutencao, pm.n_item_manutencao,
    pm.n_ultima_ordem, pm.aberta,
    CASE WHEN pm.data_criacao::text = '0000-00-00' THEN NULL ELSE pm.data_criacao END AS data_criacao,
    pm.liberada,
    CASE WHEN pm.data_liberada::text = '0000-00-00' THEN NULL ELSE pm.data_liberada END AS data_liberada,
    pm.encerrada_tecnicamente,
    CASE WHEN pm.data_encerrada_tecnicamente::text = '0000-00-00' THEN NULL ELSE pm.data_encerrada_tecnicamente END AS data_encerrada_tecnicamente,
    pm.encerrada_comercialmente,
    CASE WHEN pm.data_encerrada_comercialmente::text = '0000-00-00' THEN NULL ELSE pm.data_encerrada_comercialmente END AS data_encerrada_comercialmente,
    pm.marcado_eliminacao, pm.check_pipefy,
    CASE WHEN pm.dt_inicio_base::text = '0000-00-00' THEN NULL ELSE pm.dt_inicio_base END AS dt_inicio_base,
    CASE WHEN pm.dt_fim_base::text = '0000-00-00' THEN NULL ELSE pm.dt_fim_base END AS dt_fim_base,
    CASE WHEN pm.dt_venc_final::text = '0000-00-00' THEN NULL ELSE pm.dt_venc_final END AS dt_venc_final

FROM base_resb resb
LEFT JOIN rkpf ON resb.rsnum = rkpf.rsnum
LEFT JOIN aufk ON rkpf.aufnr = aufk.aufnr
LEFT JOIN tipos_ordem ON aufk.auart = tipos_ordem.auart
LEFT JOIN csks ON aufk.kostl = csks.kostl
LEFT JOIN cepct ON csks.prctr = cepct.prctr
LEFT JOIN aprovacao_reservas ar ON resb.chave_reserva = ar.chave
LEFT JOIN rsadd ON resb.rsnum = rsadd.rsnum AND resb.rspos = rsadd.rspos
LEFT JOIN lips ON resb.rsnum = lips.rsnum AND resb.rspos = lips.rspos
LEFT JOIN anlh ON aufk.kostl = TRIM(anlh.anln1)
LEFT JOIN makt ON TRIM(LEADING '0' FROM TRIM(resb.matnr)) = TRIM(LEADING '0' FROM TRIM(makt.matnr)) AND makt.spras = 'P'
LEFT JOIN lotes_ewm_agrupados ewm ON TRIM(LEADING '0' FROM TRIM(resb.matnr)) = ewm.material
LEFT JOIN pm_ordem_manutencao_cabecalho_v2 pm ON CAST(aufk.aufnr AS BIGINT) = CAST(pm.n_ordem AS BIGINT)
LEFT JOIN usuarios_sap u_criador_res ON TRIM(rkpf.usnam) = u_criador_res.bname_chave
LEFT JOIN usuarios_sap u_mod_res ON TRIM(rkpf.lastchangedbyuser) = u_mod_res.bname_chave
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _install_safe_date_casters() -> None:
    """SAP tem datas inválidas (ex.: ano 0000/-2) que estouram o conversor do
    psycopg2 com `ValueError: year -2 is out of range`. Registramos casters
    tolerantes: tenta o conversor padrão, senão devolve a string crua (ou None)."""
    from psycopg2 import extensions as _ext

    def make_safe(oids, base):
        def caster(value, cur):
            if value is None:
                return None
            try:
                return base(value, cur)
            except Exception:
                s = str(value).strip()
                return s or None

        register = _ext.new_type(oids, "SAFE_DT", caster)
        _ext.register_type(register)

    # date, timestamp, timestamptz, time, timetz
    make_safe((1082,), _ext.DATE)
    make_safe((1114, 1184), _ext.PYDATETIME)
    make_safe((1083, 1266), _ext.TIME)


_install_safe_date_casters()


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


def to_payload(row: dict) -> dict:
    return {k: jsonable(v) for k, v in row.items()}


def _is_recovery_conflict(err: Exception) -> bool:
    """Réplica de leitura (hot standby) cancela queries longas durante replay do WAL."""
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
        application_name="sync_reservas_separacao",
    )
    try:
        # Cursor client-side: lê tudo de uma vez e libera o snapshot da réplica
        # o mais rápido possível (cursor nomeado mantinha a transação aberta).
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
            print(f"  ! conflito com recovery na réplica SAP (tentativa {i + 1}/{attempts}); "
                  f"aguardando {wait}s e repetindo…", flush=True)
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
            "User-Agent": "sync_reservas_separacao/1.0",
        },
    )
    try:
        with urlrequest.urlopen(req, timeout=300) as resp:
            payload = resp.read().decode("utf-8") or "{}"
            return {"status": resp.status, "body": payload}
    except HTTPError as e:
        return {"status": e.code, "body": e.read().decode("utf-8", errors="replace")}
    except URLError as e:
        return {"status": 0, "body": str(e)}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    started_at = now_utc_iso()
    print(f"[{started_at}] sync_reservas_separacao — iniciando")
    print(f"  SAP: {SAP_DB_USER}@{SAP_DB_HOST}:{SAP_DB_PORT}/{SAP_DB_NAME}")
    print(f"  Destino: {APP_BASE_URL}/api/public/hooks/sync-reservas-separacao")

    try:
        rows = fetch_postgres(RESERVAS_SEPARACAO_QUERY)
    except Exception as e:
        msg = f"Falha lendo SAP: {e}"
        print(msg, file=sys.stderr)
        traceback.print_exc()
        resp = post_json(
            "/api/public/hooks/sync-reservas-separacao",
            {"started_at": started_at, "error": msg},
        )
        print(f"  → status={resp['status']} body={resp['body'][:300]}")
        return 1

    print(f"  Linhas lidas: {len(rows)}")
    payload_rows = [to_payload(r) for r in rows]
    resp = post_json(
        "/api/public/hooks/sync-reservas-separacao",
        {"started_at": started_at, "rows": payload_rows},
    )
    print(f"  → status={resp['status']} body={resp['body'][:500]}")
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
    """Nunca deixa o painel preso em 'running': qualquer crash vira erro no webhook."""
    try:
        return main()
    except Exception as e:  # noqa: BLE001
        traceback.print_exc()
        try:
            post_json(
                "/api/public/hooks/sync-reservas-separacao",
                {"started_at": now_utc_iso(), "error": f"Falha inesperada: {e}"},
            )
        except Exception:  # noqa: BLE001
            pass
        return 1


if __name__ == "__main__":
    sys.exit(_safe_main())
