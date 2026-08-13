"""
sync_patio_total.py (rev: detalhes-operacoes)
===================

Roda no GitHub Actions (.github/workflows/sync-patio.yml).

Lê o MySQL da ARMAC (UCA + telemetria) e envia os registros via POST
para os endpoints públicos do app na Lovable Cloud. O app cuida de
todos os writes no Postgres — não precisamos da senha do banco.

Variáveis de ambiente (GitHub Secrets):

  ARMAC_DB_HOST, ARMAC_DB_PORT, ARMAC_DB_USER, ARMAC_DB_PASSWORD
  UCA_DB_NAME           (default: fastfield)
  TELEMETRIA_DB_NAME    (default: total_integration)

  APP_BASE_URL          (default: https://gestaofilialbh.lovable.app)
  SYNC_WEBHOOK_SECRET   (string compartilhada com o app)
"""

from __future__ import annotations

import json
import os
# Remove espacos/quebras de linha acidentais colados nos secrets do GitHub.
for _k, _v in list(os.environ.items()):
    if isinstance(_v, str) and _v != _v.strip():
        os.environ[_k] = _v.strip()

import re
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError

import pymysql

try:
    import psycopg2
    import psycopg2.extras
    _HAS_PG = True
except ImportError:
    _HAS_PG = False




# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# UCA (fastfield)
UCA_DB_HOST = os.environ.get("ARMAC_DB_HOST") or ""
UCA_DB_PORT = int(os.environ.get("ARMAC_DB_PORT", "3306"))
UCA_DB_USER = os.environ.get("ARMAC_DB_USER") or ""
UCA_DB_PASSWORD = os.environ.get("ARMAC_DB_PASSWORD") or ""
UCA_DB_NAME = os.environ.get("UCA_DB_NAME", "fastfield")

# Telemetria (total_integration) — host/porta/credenciais separados; cai pra UCA se não setar
TELEMETRIA_DB_HOST = os.environ.get("TELEMETRIA_DB_HOST") or UCA_DB_HOST
TELEMETRIA_DB_PORT = int(os.environ.get("TELEMETRIA_DB_PORT", str(UCA_DB_PORT)))
TELEMETRIA_DB_USER = os.environ.get("TELEMETRIA_DB_USER") or UCA_DB_USER
TELEMETRIA_DB_PASSWORD = os.environ.get("TELEMETRIA_DB_PASSWORD") or UCA_DB_PASSWORD
TELEMETRIA_DB_NAME = os.environ.get("TELEMETRIA_DB_NAME", "total_integration")

APP_BASE_URL = (os.environ.get("APP_BASE_URL") or "https://gestaofilialbh.lovable.app").rstrip("/")
WEBHOOK_SECRET = os.environ.get("SYNC_WEBHOOK_SECRET") or ""

if not UCA_DB_HOST or not UCA_DB_USER or not UCA_DB_PASSWORD:
    print("ERRO: ARMAC_DB_HOST/USER/PASSWORD não definidos", file=sys.stderr)
    sys.exit(2)
if not WEBHOOK_SECRET:
    print("ERRO: SYNC_WEBHOOK_SECRET não definido", file=sys.stderr)
    sys.exit(2)

# SAP PM (Postgres réplica) — opcional. Se não setar, o bloco SAP é pulado.
# SAP PM (Postgres réplica HANA) — opcional. Aceita HANA_DB_* ou SAP_DB_*.
SAP_DB_HOST = os.environ.get("HANA_DB_HOST") or os.environ.get("SAP_DB_HOST") or ""
_sap_port_raw = os.environ.get("HANA_DB_PORT") or os.environ.get("SAP_DB_PORT")
SAP_DB_PORT = int(_sap_port_raw) if _sap_port_raw else 5432
SAP_DB_USER = os.environ.get("HANA_DB_USER") or os.environ.get("SAP_DB_USER") or ""
SAP_DB_PASSWORD = os.environ.get("HANA_DB_PASSWORD") or os.environ.get("SAP_DB_PASSWORD") or ""
SAP_DB_NAME = os.environ.get("HANA_DB_NAME") or os.environ.get("SAP_DB_NAME") or ""
SAP_ENABLED = bool(SAP_DB_HOST and SAP_DB_USER and SAP_DB_PASSWORD and SAP_DB_NAME)


SAP_QUERY = """
WITH classificacao_os AS (
  SELECT
    pm.*,
    CASE
      WHEN pm.cod_tipo_atividade = 'PRP' OR pm.tipo_atividade ILIKE '%%prep%%' THEN 'Preparação'
      WHEN pm.cod_tipo_atividade = 'OFI' OR pm.tipo_atividade ILIKE '%%ofi%%'  THEN 'Oficina'
      ELSE 'Outro'
    END AS tipo_manutencao
  FROM pm_ordem_manutencao_cabecalho_v2 pm
  WHERE (pm.cod_centro_trabalho LIKE '%%BHZ%%' OR pm.cod_centro_trabalho LIKE '%%BET%%')
    AND pm.data_criacao >= '2024-01-01'
),
ultima_os_por_tipo AS (
  SELECT
    c.n_equipamento AS ativo,
    TRIM(LEADING '0' FROM c.n_ordem) AS ordem,
    c.tipo_manutencao,
    c.tipo_atividade,
    c.prioridade AS criticidade,
    c.status_usuario AS status_os_usuario,
    c.data_criacao,
    CASE
      WHEN c.encerrada_comercialmente = 'X' THEN '3 - Encerrada Comercial'
      WHEN c.encerrada_tecnicamente   = 'X' THEN '2 - Concluída Tecnicamente'
      WHEN c.liberada                 = 'X' THEN '1 - Liberada'
      ELSE '0 - Criada / Aberta'
    END AS fase_atual,
    CASE
      WHEN c.cod_centro_trabalho LIKE '%%BHZ%%' THEN 'BHZ'
      WHEN c.cod_centro_trabalho LIKE '%%BET%%' THEN 'BET'
      ELSE c.cod_centro_trabalho
    END AS filial,
    ROW_NUMBER() OVER (PARTITION BY c.n_equipamento, c.tipo_manutencao ORDER BY c.data_criacao DESC) AS rn
  FROM classificacao_os c
  WHERE c.tipo_manutencao IN ('Oficina', 'Preparação')
)
SELECT ativo, filial, ordem, tipo_manutencao, tipo_atividade,
       criticidade, fase_atual AS fase_os, status_os_usuario AS status_usuario,
       data_criacao
FROM ultima_os_por_tipo
WHERE rn = 1
ORDER BY ativo, tipo_manutencao;
"""


SAP_OS_DETALHES_QUERY = """
WITH base_os AS (
  SELECT
    AUFK.AUFNR,
    AUFK.KTEXT AS descricao_os,
    AUFK.ERDAT AS data_criacao,
    AUFK.ERNAM AS autor_os,
    AUFK.IDAT1 AS data_liberacao,
    AFIH.EQUNR AS ativo,
    CRHD_CAB.ARBPL AS centro_trabalho_cabecalho,
    CASE
      WHEN AFIH.ILART = 'PRP' OR AFIH.ILART LIKE '%%PRP%%' THEN 'Preparação'
      WHEN AFIH.ILART = 'OFI' OR AFIH.ILART LIKE '%%OFI%%' THEN 'Oficina'
      ELSE 'Outro'
    END AS tipo_manutencao
  FROM AUFK
  INNER JOIN AFIH ON AUFK.AUFNR = AFIH.AUFNR
  LEFT JOIN CRHD CRHD_CAB ON AFIH.GEWRK = CRHD_CAB.OBJID
  WHERE AUFK.ERDAT >= '2024-01-01'
    AND (CRHD_CAB.ARBPL LIKE '%%BHZ%%' OR CRHD_CAB.ARBPL LIKE '%%BET%%')
),
os_campeas AS (
  SELECT * FROM (
    SELECT c.*,
      ROW_NUMBER() OVER (PARTITION BY c.ativo, c.tipo_manutencao ORDER BY c.data_criacao DESC, c.AUFNR DESC) AS rn
    FROM base_os c
    WHERE c.tipo_manutencao IN ('Oficina', 'Preparação')
  ) sub WHERE rn = 1
),
dados_planejamento AS (
  SELECT
    OS.AUFNR,
    MAX(AFKO.GSTRP) AS data_inicio_programada,
    MAX(AFKO.GLTRP) AS data_fim_programada,
    MAX(AFKO.RSNUM) AS RSNUM,
    MAX(AFKO.AUFPL) AS AUFPL
  FROM os_campeas OS
  INNER JOIN AFKO ON OS.AUFNR = AFKO.AUFNR
  GROUP BY OS.AUFNR
),
operacoes AS (
  SELECT
    D.AUFNR,
    AFVC.VORNR AS num_operacao,
    AFVC.LTXA1 AS descricao_operacao,
    CRHD_OP.ARBPL AS centro_trabalho_operacao,
    AFVV.ARBEI AS tempo_previsto_h,
    AFVC.OBJNR
  FROM dados_planejamento D
  INNER JOIN AFVC ON D.AUFPL = AFVC.AUFPL
  LEFT JOIN AFVV ON AFVC.AUFPL = AFVV.AUFPL AND AFVC.APLZL = AFVV.APLZL
  LEFT JOIN CRHD CRHD_OP ON AFVC.ARBID = CRHD_OP.OBJID
),
status_op AS (
  SELECT OP.OBJNR, STRING_AGG(ST.TXT04, ' ') AS status_operacao
  FROM operacoes OP
  INNER JOIN JEST J ON OP.OBJNR = J.OBJNR AND J.INACT = ''
  INNER JOIN TJ02T ST ON J.STAT = ST.ISTAT AND ST.SPRAS IN ('P', 'PT', 'pt', 'p')
  GROUP BY OP.OBJNR
)
SELECT
  OS.ativo AS ativo,
  TRIM(LEADING '0' FROM OS.AUFNR) AS ordem,
  OS.tipo_manutencao,
  OS.descricao_os,
  OS.centro_trabalho_cabecalho AS centro_trabalho_responsavel,
  DP.data_inicio_programada,
  DP.data_fim_programada,
  OS.data_liberacao,
  OS.autor_os,
  OP.num_operacao,
  OP.descricao_operacao,
  OP.centro_trabalho_operacao,
  OP.tempo_previsto_h,
  SOP.status_operacao
FROM os_campeas OS
INNER JOIN dados_planejamento DP ON OS.AUFNR = DP.AUFNR
INNER JOIN operacoes OP ON OS.AUFNR = OP.AUFNR
LEFT JOIN status_op SOP ON OP.OBJNR = SOP.OBJNR
ORDER BY OS.ativo, OS.AUFNR, OP.num_operacao;
"""


EQUIPAMENTOS_QUERY = """
SELECT
    bu                              AS tipo_holo,
    numero_armac                    AS ativo,
    descricao                       AS descricao,
    chassi                          AS chassi,
    marca                           AS marca,
    modelo                          AS modelo,
    tipo                            AS tipo_equipamento,
    grupo                           AS porte_tonelagem,
    ano_fabricacao                  AS ano_fabricacao,
    status_ativo                    AS status_ativo,
    local_instalacao                AS local_instalacao
FROM armac.fi_ativos
WHERE status_ativo = 'Ativo'
ORDER BY numero_armac;
"""


SUPRIMENTOS_QUERY = """
WITH ultima_os AS (
    SELECT
        n_equipamento AS ativo,
        TRIM(LEADING '0' FROM n_ordem) AS ordem_tratada,
        LPAD(TRIM(n_ordem), 12, '0') AS ordem_sap
    FROM (
        SELECT
            n_equipamento,
            n_ordem,
            ROW_NUMBER() OVER(
                PARTITION BY n_equipamento,
                CASE
                    WHEN cod_tipo_atividade = 'PRP' OR tipo_atividade ILIKE '%%prep%%' THEN 'Preparação'
                    WHEN cod_tipo_atividade = 'OFI' OR tipo_atividade ILIKE '%%ofi%%' THEN 'Oficina'
                END
                ORDER BY data_criacao DESC
            ) as rn
        FROM pm_ordem_manutencao_cabecalho_v2
        WHERE (cod_centro_trabalho LIKE '%%BHZ%%' OR cod_centro_trabalho LIKE '%%BET%%')
          AND data_criacao >= '2024-01-01'
          AND (cod_tipo_atividade IN ('PRP', 'OFI') OR tipo_atividade ILIKE '%%prep%%' OR tipo_atividade ILIKE '%%ofi%%')
    ) sub
    WHERE rn = 1
),
itens_brutos AS (
    SELECT
        os.ativo,
        os.ordem_tratada AS ordem,
        LTRIM(TRIM(E.MATNR), '0') AS cod_sap,
        E.TXZ01 AS desc_compra_direta,
        E.MENGE AS qtd_req,
        LTRIM(TRIM(P.EBELN), '0') AS pedido,
        CAST(NULL AS VARCHAR) AS num_reserva,
        'compra'::text AS origem,
        E.KNTTP AS knttp
    FROM ultima_os os
    JOIN EBKN ACC ON ACC.AUFNR = os.ordem_sap
    JOIN EBAN E   ON E.BANFN = ACC.BANFN AND E.BNFPO = ACC.BNFPO
    LEFT JOIN EKPO P ON P.BANFN = E.BANFN AND P.BNFPO = E.BNFPO

    UNION ALL

    SELECT
        os.ativo,
        os.ordem_tratada AS ordem,
        LTRIM(TRIM(R.MATNR), '0') AS cod_sap,
        CAST(NULL AS VARCHAR) AS desc_compra_direta,
        R.BDMNG AS qtd_req,
        LTRIM(TRIM(P.EBELN), '0') AS pedido,
        CASE
            WHEN R.POSTP = 'N' THEN CAST(NULL AS VARCHAR)
            ELSE LTRIM(TRIM(R.RSNUM), '0')
        END AS num_reserva,
        CASE
            WHEN R.POSTP = 'N' THEN 'compra'::text
            ELSE 'reserva'::text
        END AS origem,
        E.KNTTP AS knttp
    FROM ultima_os os
    JOIN RESB R   ON R.AUFNR = os.ordem_sap
    LEFT JOIN EBAN E ON E.BANFN = R.BANFN AND E.BNFPO = R.BNFPO
    LEFT JOIN EKPO P ON P.BANFN = E.BANFN AND P.BNFPO = E.BNFPO
    WHERE (R.XLOEK IS NULL OR TRIM(R.XLOEK) = '')
),
itens_deduplicados AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ordem, cod_sap
            ORDER BY
                CASE WHEN origem = 'compra' THEN 1 ELSE 2 END,
                CASE WHEN pedido IS NOT NULL THEN 1 ELSE 2 END,
                CASE WHEN num_reserva IS NOT NULL THEN 1 ELSE 2 END
        ) AS rn
    FROM itens_brutos
    WHERE cod_sap IS NOT NULL AND cod_sap <> ''
)
SELECT
    itens.ativo         AS ativo,
    itens.ordem         AS ordem,
    itens.cod_sap       AS cod_sap,
    COALESCE(M.MAKTX, itens.desc_compra_direta, 'Sem Descrição') AS descricao,
    itens.qtd_req       AS qtd_req,
    itens.num_reserva   AS num_reserva,
    itens.pedido        AS pedido,
    itens.origem        AS origem,
    NULLIF(TRIM(COALESCE(itens.knttp,'')),'') AS knttp,
    CASE
        WHEN COALESCE(TRIM(itens.knttp),'') = '' THEN 'estoque'
        WHEN TRIM(itens.knttp) = 'K' THEN 'centro_custo'
        ELSE 'consumo'
    END AS destinacao
FROM itens_deduplicados itens
LEFT JOIN MAKT M ON LTRIM(TRIM(M.MATNR), '0') = itens.cod_sap AND M.SPRAS IN ('P', 'PT')
WHERE itens.rn = 1
ORDER BY itens.ativo, itens.ordem;
"""


# Fluxo completo RC -> PC dos grupos de compradores da filial (201/220/251)
# e dos compradores nominais. Cobre pedidos com OS (ativo do equipamento) e
# pedidos de estoque/centro de custo (ativo 'ESTOQUE').
SUPRIMENTOS_COMPRAS_QUERY = """
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
  CAST(NULL AS VARCHAR) AS num_reserva,
  'compra'::text AS origem,
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
  AND E.BADAT >= TO_CHAR(CURRENT_DATE - __DIAS_INICIO__, 'YYYYMMDD')
  AND E.BADAT <= TO_CHAR(CURRENT_DATE - __DIAS_FIM__, 'YYYYMMDD')
  AND TRIM(LEADING '0' FROM TRIM(E.MATNR)) <> ''
ORDER BY E.BANFN ASC;
"""


def suprimentos_compras_query(dias_inicio: int, dias_fim: int) -> str:
    return (
        SUPRIMENTOS_COMPRAS_QUERY
        .replace("__DIAS_INICIO__", str(int(dias_inicio)))
        .replace("__DIAS_FIM__", str(int(dias_fim)))
    )









UCA_QUERY = """
    SELECT
        UPPER(TRIM(Atual.n_armac))   AS n_armac,
        Atual.tipo_equipamento       AS equipamento,
        UPPER(TRIM(Atual.marca))     AS marca,
        UPPER(TRIM(Atual.modelo))    AS modelo,
        Atual.cliente                AS cliente,
        Atual.situacao               AS situacao,
        Atual.created_at_form        AS data_entrada,
        Atual.horimetro              AS horimetro,
        Atual.filial                 AS filial
    FROM fastfield.relatorio_entrada_saida_uca Atual
    INNER JOIN (
        SELECT n_armac, MAX(created_at_form) AS ultima_data
        FROM fastfield.relatorio_entrada_saida_uca
        WHERE n_armac IS NOT NULL
        GROUP BY n_armac
    ) UltimoRegistro
      ON Atual.n_armac = UltimoRegistro.n_armac
     AND Atual.created_at_form = UltimoRegistro.ultima_data
    WHERE Atual.tipo_relatorio = 'Entrada'
      AND (Atual.filial LIKE '%%BH%%' OR Atual.filial LIKE '%%Belo Horizonte%%')
"""

# Histórico completo (Entrada + Saída) das filiais BH — alimenta
# public.ativo_uca_eventos, preservando quando cada ativo entrou/saiu.
UCA_HISTORICO_QUERY = """
    SELECT
        UPPER(TRIM(r.n_armac))  AS n_armac,
        r.tipo_relatorio        AS tipo_relatorio,
        r.created_at_form       AS ts,
        r.filial                AS filial,
        r.cliente               AS cliente,
        r.situacao              AS situacao,
        r.horimetro             AS horimetro,
        r.tipo_equipamento      AS descricao
    FROM fastfield.relatorio_entrada_saida_uca r
    WHERE r.n_armac IS NOT NULL
      AND r.n_armac <> ''
      AND r.created_at_form IS NOT NULL
      AND r.tipo_relatorio IN ('Entrada', 'Saída', 'Saida')
      AND EXISTS (
          SELECT 1
          FROM fastfield.relatorio_entrada_saida_uca bh
          WHERE UPPER(TRIM(bh.n_armac)) = UPPER(TRIM(r.n_armac))
            AND (bh.filial LIKE '%%BH%%' OR bh.filial LIKE '%%Belo Horizonte%%')
      )
"""

HORIMETRO_QUERY = """
    SELECT
        UPPER(TRIM(h.armac_code))     AS n_armac,
        h.event_datetime              AS data_comunicacao,
        COALESCE(h.equipment_hourmeter, h.panel_hourmeter) AS horimetro_sistema,
        h.contract                    AS contrato,
        h.updated_at                  AS atualizado_em
    FROM total_integration.hourmeter h
    INNER JOIN (
        SELECT armac_code, MAX(event_datetime) AS max_dt
        FROM total_integration.hourmeter
        WHERE armac_code IS NOT NULL AND armac_code <> ''
        GROUP BY armac_code
    ) ult
      ON h.armac_code = ult.armac_code
     AND h.event_datetime = ult.max_dt
"""

# Leituras que o pipeline da telemetria rejeitou (ex.: HOURMETER_NEGATIVE após
# troca de rastreador). O horímetro não é confiável, mas PROVA que o ativo
# está comunicando — usamos só como data de comunicação.
HORIMETRO_INVALIDO_QUERY = """
    SELECT
        UPPER(TRIM(i.armac_code))  AS n_armac,
        i.event_datetime           AS comunicacao_rastreador_em,
        i.hourmeter                AS horimetro_rastreador,
        i.source                   AS fonte_rastreador,
        i.error_code               AS motivo_rastreador
    FROM total_integration.hourmeter_invalid i
    INNER JOIN (
        SELECT armac_code, MAX(event_datetime) AS max_dt
        FROM total_integration.hourmeter_invalid
        WHERE armac_code IS NOT NULL AND armac_code <> ''
        GROUP BY armac_code
    ) ult
      ON i.armac_code = ult.armac_code
     AND i.event_datetime = ult.max_dt
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# UCA/SAP devolvem datas SEM fuso, mas o relógio delas é o de Brasília.
# Antes elas eram carimbadas como UTC e chegavam 3h adiantadas no app
# (saída da UCA, datas de OS). Todo datetime "ingênuo" agora é tratado
# como America/Sao_Paulo (-03:00; o Brasil não tem mais horário de verão).
TZ_BR = timezone(timedelta(hours=-3))


def assume_br(v: datetime) -> datetime:
    return v.replace(tzinfo=TZ_BR) if v.tzinfo is None else v


def jsonable(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, datetime):
        return assume_br(v).isoformat()
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day, tzinfo=TZ_BR).isoformat()
    return str(v)


def to_payload(row: dict) -> dict:
    return {k: jsonable(v) for k, v in row.items()}


SAP_DATE_FIELDS = {
    "data_criacao",
    "data_inicio_programada",
    "data_fim_programada",
    "data_liberacao",
}


def clean_sap_date(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return assume_br(v).isoformat()
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day, tzinfo=TZ_BR).isoformat()


    s = str(v).strip()
    if not s or s.lower() in {"none", "null", "nan", "nat", "infinity", "-infinity"}:
        return None

    digits = re.sub(r"\D", "", s)
    if not digits or set(digits) == {"0"}:
        return None
    if re.fullmatch(r"\d{8}", digits):
        try:
            return datetime.strptime(digits, "%Y%m%d").replace(tzinfo=TZ_BR).isoformat()
        except ValueError:
            return None

    try:
        parsed = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return assume_br(parsed).isoformat()
    except ValueError:
        return None


def to_sap_payload(row: dict) -> dict:
    payload = to_payload(row)
    for field in SAP_DATE_FIELDS:
        if field in payload:
            payload[field] = clean_sap_date(payload[field])
    return payload


def fetch_mysql(database: str, query: str, user: str, password: str, host: str, port: int) -> list[dict]:
    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        connect_timeout=20,
        read_timeout=120,
        cursorclass=pymysql.cursors.DictCursor,
        charset="utf8mb4",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            return list(cur.fetchall())
    finally:
        conn.close()


def fetch_postgres(database: str, query: str, user: str, password: str, host: str, port: int) -> list[dict]:
    if not _HAS_PG:
        raise RuntimeError("psycopg2 não instalado — adicione psycopg2-binary nas dependências")
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=database,
        connect_timeout=20,
        sslmode="require",
        application_name="sync_patio_total",
    )
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()




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
            "User-Agent": "sync_patio_total/1.0",
        },
    )
    try:
        with urlrequest.urlopen(req, timeout=180) as resp:
            txt = resp.read().decode("utf-8", errors="replace")
            try:
                return json.loads(txt)
            except json.JSONDecodeError:
                return {"ok": False, "raw": txt[:500]}
    except HTTPError as e:
        body_txt = e.read().decode("utf-8", errors="replace") if e.fp else ""
        raise RuntimeError(f"HTTP {e.code} em {path}: {body_txt[:500]}")
    except URLError as e:
        raise RuntimeError(f"Falha de rede em {path}: {e}")


def report_failure(path: str, started_at: str, exc: Exception) -> None:
    try:
        post_json(path, {"started_at": started_at, "error": str(exc)})
    except Exception as report_exc:  # noqa: BLE001
        print(f"[AVISO] não consegui registrar falha no app: {report_exc}", file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    print(f"[{now_utc_iso()}] sync_patio_total iniciado", flush=True)
    summary: dict[str, Any] = {}
    exit_code = 0

    # --- UCA ---
    started_at = now_utc_iso()
    try:
        rows = fetch_mysql(UCA_DB_NAME, UCA_QUERY, UCA_DB_USER, UCA_DB_PASSWORD, UCA_DB_HOST, UCA_DB_PORT)
        try:
            eventos = fetch_mysql(UCA_DB_NAME, UCA_HISTORICO_QUERY, UCA_DB_USER, UCA_DB_PASSWORD, UCA_DB_HOST, UCA_DB_PORT)
        except Exception as exc:  # noqa: BLE001
            print(f"[UCA] histórico falhou (não-fatal) · {exc}", file=sys.stderr, flush=True)
            eventos = []
        payload = {
            "started_at": started_at,
            "rows": [to_payload(r) for r in rows],
            "eventos": [to_payload(r) for r in eventos],
        }
        result = post_json("/api/public/hooks/sync-ativos-bh", payload)
        if not result.get("ok"):
            raise RuntimeError(f"app respondeu sem ok: {result}")
        summary["uca"] = result
        print(f"[UCA] ok · {result.get('message') or result}", flush=True)
    except Exception as exc:  # noqa: BLE001
        summary["uca"] = {"ok": False, "error": str(exc)}
        print(f"[UCA] erro · {exc}", file=sys.stderr, flush=True)
        report_failure("/api/public/hooks/sync-ativos-bh", started_at, exc)
        exit_code = 1

    # --- TELEMETRIA ---
    started_at = now_utc_iso()
    try:
        rows = fetch_mysql(TELEMETRIA_DB_NAME, HORIMETRO_QUERY, TELEMETRIA_DB_USER, TELEMETRIA_DB_PASSWORD, TELEMETRIA_DB_HOST, TELEMETRIA_DB_PORT)
        rows_payload = [to_payload(r) for r in rows]

        # Mescla a última comunicação "inválida" (rastreador comunicando com
        # horímetro rejeitado) para não marcar o ativo como sem comunicação.
        try:
            inval = fetch_mysql(TELEMETRIA_DB_NAME, HORIMETRO_INVALIDO_QUERY, TELEMETRIA_DB_USER, TELEMETRIA_DB_PASSWORD, TELEMETRIA_DB_HOST, TELEMETRIA_DB_PORT)
            por_ativo = {str(r.get("n_armac") or "").strip().upper(): r for r in rows_payload}
            for iv in inval:
                p = to_payload(iv)
                k = str(p.get("n_armac") or "").strip().upper()
                if not k:
                    continue
                alvo = por_ativo.get(k)
                if alvo is None:
                    alvo = {"n_armac": k}
                    por_ativo[k] = alvo
                    rows_payload.append(alvo)
                alvo["comunicacao_rastreador_em"] = p.get("comunicacao_rastreador_em")
                alvo["horimetro_rastreador"] = p.get("horimetro_rastreador")
                alvo["fonte_rastreador"] = p.get("fonte_rastreador")
                alvo["motivo_rastreador"] = p.get("motivo_rastreador")
            print(f"[TELE] {len(inval)} leituras rejeitadas mescladas", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"[TELE][AVISO] falha ao ler hourmeter_invalid: {exc}", file=sys.stderr, flush=True)

        payload = {"started_at": started_at, "rows": rows_payload}
        result = post_json("/api/public/hooks/sync-horimetros", payload)
        if not result.get("ok"):
            raise RuntimeError(f"app respondeu sem ok: {result}")
        summary["telemetria"] = result
        print(f"[TELE] ok · {result.get('message') or result}", flush=True)
    except Exception as exc:  # noqa: BLE001
        summary["telemetria"] = {"ok": False, "error": str(exc)}
        print(f"[TELE] erro · {exc}", file=sys.stderr, flush=True)
        report_failure("/api/public/hooks/sync-horimetros", started_at, exc)
        exit_code = 1

    # --- SAP PM ---
    if SAP_ENABLED:
        started_at = now_utc_iso()
        try:
            rows = fetch_postgres(SAP_DB_NAME, SAP_QUERY, SAP_DB_USER, SAP_DB_PASSWORD, SAP_DB_HOST, SAP_DB_PORT)
            rows_payload = [to_sap_payload(r) for r in rows]

            # Query complementar (cabeçalho + operações). Não bloqueante: se
            # falhar, manda só o payload antigo.
            operacoes_payload: list[dict] = []
            try:
                det_rows = fetch_postgres(SAP_DB_NAME, SAP_OS_DETALHES_QUERY, SAP_DB_USER, SAP_DB_PASSWORD, SAP_DB_HOST, SAP_DB_PORT)
                det = [to_sap_payload(r) for r in det_rows]

                # Cabeçalho extra por (ativo, ordem) — pega a 1ª ocorrência
                extras: dict[tuple, dict] = {}
                for d in det:
                    k = (str(d.get("ativo") or "").strip(), str(d.get("ordem") or "").strip())
                    if k[0] and k[1] and k not in extras:
                        extras[k] = {
                            "descricao_os": d.get("descricao_os"),
                            "centro_trabalho_responsavel": d.get("centro_trabalho_responsavel"),
                            "data_inicio_programada": d.get("data_inicio_programada"),
                            "data_fim_programada": d.get("data_fim_programada"),
                            "data_liberacao": d.get("data_liberacao"),
                            "autor_os": d.get("autor_os"),
                        }

                for r in rows_payload:
                    k = (str(r.get("ativo") or "").strip(), str(r.get("ordem") or r.get("numero_ordem") or "").strip())
                    extra = extras.get(k)
                    if extra:
                        r.update(extra)

                # Operações distintas
                seen = set()
                for d in det:
                    k = (
                        str(d.get("ativo") or "").strip(),
                        str(d.get("ordem") or "").strip(),
                        str(d.get("num_operacao") or "").strip(),
                    )
                    if not all(k) or k in seen:
                        continue
                    seen.add(k)
                    operacoes_payload.append({
                        "ativo": k[0],
                        "ordem": k[1],
                        "num_operacao": k[2],
                        "descricao_operacao": d.get("descricao_operacao"),
                        "centro_trabalho_operacao": d.get("centro_trabalho_operacao"),
                        "tempo_previsto_h": d.get("tempo_previsto_h"),
                        "status_operacao": d.get("status_operacao"),
                    })
                print(f"[SAP] detalhes · {len(extras)} cabeçalhos enriquecidos · {len(operacoes_payload)} operações", flush=True)
            except Exception as exc:  # noqa: BLE001
                print(f"[SAP] detalhes pulado · {exc}", file=sys.stderr, flush=True)

            payload = {"started_at": started_at, "rows": rows_payload, "operacoes": operacoes_payload}
            result = post_json("/api/public/hooks/sync-sap-os", payload)
            if not result.get("ok"):
                raise RuntimeError(f"app respondeu sem ok: {result}")
            summary["sap"] = result
            print(f"[SAP] ok · {result.get('message') or result}", flush=True)
        except Exception as exc:  # noqa: BLE001
            summary["sap"] = {"ok": False, "error": str(exc)}
            print(f"[SAP] erro · {exc}", file=sys.stderr, flush=True)
            report_failure("/api/public/hooks/sync-sap-os", started_at, exc)
            exit_code = 1
    else:
        print("[SAP] pulado · HANA_DB_* não configurado", flush=True)
        summary["sap"] = {"ok": True, "skipped": True}

    # --- EQUIPAMENTOS (armac.fi_ativos no mesmo HANA) ---
    if SAP_ENABLED:
        started_at = now_utc_iso()
        try:
            rows = fetch_postgres(SAP_DB_NAME, EQUIPAMENTOS_QUERY, SAP_DB_USER, SAP_DB_PASSWORD, SAP_DB_HOST, SAP_DB_PORT)
            payload = {"started_at": started_at, "rows": [to_payload(r) for r in rows]}
            result = post_json("/api/public/hooks/sync-equipamentos-data", payload)
            if not result.get("ok"):
                raise RuntimeError(f"app respondeu sem ok: {result}")
            summary["equipamentos"] = result
            print(f"[EQUIP] ok · {result.get('message') or result}", flush=True)
        except Exception as exc:  # noqa: BLE001
            summary["equipamentos"] = {"ok": False, "error": str(exc)}
            print(f"[EQUIP] erro · {exc}", file=sys.stderr, flush=True)
            report_failure("/api/public/hooks/sync-equipamentos-data", started_at, exc)
            exit_code = 1
    else:
        print("[EQUIP] pulado · HANA_DB_* não configurado", flush=True)
        summary["equipamentos"] = {"ok": True, "skipped": True}

    # --- SUPRIMENTOS (itens das OS no mesmo HANA) ---
    if SAP_ENABLED:
        started_at = now_utc_iso()
        try:
            rows = fetch_postgres(SAP_DB_NAME, SUPRIMENTOS_QUERY, SAP_DB_USER, SAP_DB_PASSWORD, SAP_DB_HOST, SAP_DB_PORT)
            rows_payload = [to_payload(r) for r in rows]
            payload = {"started_at": started_at, "rows": rows_payload}

            result = post_json("/api/public/hooks/sync-suprimentos-data", payload)
            if not result.get("ok"):
                raise RuntimeError(f"app respondeu sem ok: {result}")
            summary["suprimentos"] = result
            print(f"[SUPR] ok · {result.get('message') or result}", flush=True)
        except Exception as exc:  # noqa: BLE001
            summary["suprimentos"] = {"ok": False, "error": str(exc)}
            print(f"[SUPR] erro · {exc}", file=sys.stderr, flush=True)
            report_failure("/api/public/hooks/sync-suprimentos-data", started_at, exc)
            exit_code = 1
    else:
        print("[SUPR] pulado · HANA_DB_* não configurado", flush=True)
        summary["suprimentos"] = {"ok": True, "skipped": True}

    # --- SUPRIMENTOS COMPRAS (fluxo RC -> PC, alimenta Validação de Pedidos) ---
    if SAP_ENABLED:
        started_at = now_utc_iso()
        try:
            rows_por_item: dict[tuple[str, str], dict] = {}
            falhas_janelas: list[str] = []
            janelas_ok = 0
            for dias_inicio in range(180, 0, -14):
                dias_fim = max(0, dias_inicio - 14)
                janela_rows: list[dict] | None = None
                ultimo_erro: Exception | None = None
                for tentativa in range(1, 4):
                    try:
                        janela_rows = fetch_postgres(
                            SAP_DB_NAME,
                            suprimentos_compras_query(dias_inicio, dias_fim),
                            SAP_DB_USER,
                            SAP_DB_PASSWORD,
                            SAP_DB_HOST,
                            SAP_DB_PORT,
                        )
                        break
                    except Exception as exc:  # noqa: BLE001
                        ultimo_erro = exc
                        print(
                            f"[COMPRAS] janela {dias_inicio}..{dias_fim} tentativa {tentativa}/3 falhou · {exc}",
                            file=sys.stderr,
                            flush=True,
                        )
                if janela_rows is None:
                    falhas_janelas.append(
                        f"{dias_inicio}..{dias_fim}: {ultimo_erro or 'sem detalhe'}"
                    )
                    continue
                janelas_ok += 1
                for row in janela_rows:
                    chave = (
                        str(row.get("num_rc") or "").strip(),
                        str(row.get("item_rc") or "").strip(),
                    )
                    if all(chave):
                        rows_por_item[chave] = row

            if janelas_ok == 0:
                raise RuntimeError(
                    "todas as janelas RC/PC falharam: "
                    + (falhas_janelas[0] if falhas_janelas else "sem detalhe")
                )

            rows = list(rows_por_item.values())
            payload = {"started_at": started_at, "rows": [to_payload(r) for r in rows]}
            result = post_json("/api/public/hooks/sync-suprimentos-compras", payload)
            if not result.get("ok"):
                raise RuntimeError(f"app respondeu sem ok: {result}")
            summary["suprimentos_compras"] = result
            aviso = (
                f" · {len(falhas_janelas)} janela(s) serão refeitas na próxima execução"
                if falhas_janelas else ""
            )
            print(
                f"[COMPRAS] ok · {len(rows)} itens RC/PC{aviso} · {result.get('message') or result}",
                flush=True,
            )
        except Exception as exc:  # noqa: BLE001
            summary["suprimentos_compras"] = {"ok": False, "error": str(exc)}
            print(f"[COMPRAS] erro · {exc}", file=sys.stderr, flush=True)
            report_failure("/api/public/hooks/sync-suprimentos-compras", started_at, exc)
            exit_code = 1
    else:
        print("[COMPRAS] pulado · HANA_DB_* não configurado", flush=True)
        summary["suprimentos_compras"] = {"ok": True, "skipped": True}

    print(json.dumps(summary, default=str), flush=True)
    return exit_code



if __name__ == "__main__":
    sys.exit(main())
