# armac-syncs

Automações de sincronização (GitHub Actions) que alimentam o app Gestão Filial BH.
Os scripts leem os bancos de origem (MySQL ARMAC e réplica Postgres do SAP) e enviam
os dados para os webhooks do app. Nenhuma credencial fica no código — tudo via
GitHub Secrets.

## Workflows

| Workflow | Frequência | Script |
|---|---|---|
| Sync Pátio (UCA + Telemetria + SAP) | a cada hora (:00) | `sync_patio_total.py` |
| Sync MIGO (Recebimentos) | a cada hora (:15) | `sync_migo.py` |
| Sync OS Externas (Spot/Oficinas) | a cada hora (:25) | `sync_os_externas.py` |
| Sync Reservas de Separação (EWM) | a cada 30 min | `sync_reservas_separacao.py` |
| Sync Compras RC→PC | a cada hora (:35) | `sync_compras_rcpc.py` |
| Sync Pagamento de Pedidos (MIRO) | a cada hora (:35) | `sync_pagamento_pedidos.py` |
| Sync Aprovação de Pedidos | a cada hora (:45) | `sync_aprovacao_pedidos.py` |
| Sync SAP Peças Histórico | 1x/dia 06:00 UTC | `sync_sap_pecas_historico.py` |

## Secrets necessários (Settings → Secrets and variables → Actions)

- `SYNC_WEBHOOK_SECRET` — mesmo valor cadastrado no Lovable Cloud
- `APP_BASE_URL` — opcional (default: https://gestaofilialbh.lovable.app)
- `HANA_DB_HOST`, `HANA_DB_PORT`, `HANA_DB_USER`, `HANA_DB_PASSWORD`, `HANA_DB_NAME` — réplica Postgres do SAP
- `ARMAC_DB_HOST`, `ARMAC_DB_PORT`, `ARMAC_DB_USER`, `ARMAC_DB_PASSWORD` — MySQL ARMAC (UCA/telemetria)
- `MIGO_DB_HOST`, `MIGO_DB_PORT`, `MIGO_DB_USER`, `MIGO_DB_PASSWORD`, `MIGO_DB_NAME` — MySQL requisições/MIGO

Opcionais: `TELEMETRIA_DB_HOST/PORT/USER/PASSWORD`, `UCA_DB_NAME`, `TELEMETRIA_DB_NAME`.
