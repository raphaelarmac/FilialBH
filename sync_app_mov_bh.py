"""
sync_app_mov_bh.py
Lê os movimentos dos últimos 2 dias do app de movimentação (app_mov_ativos) na filial BH
(centro C006 ou GPS a até 15 km da filial) e envia para o app:
  POST {APP_BASE_URL}/api/public/hooks/sync-app-mov  (x-webhook-secret)
DESCARREGADO = entrada; CARREGADO = saída. Horários em UTC.
"""
import json, os, sys
from urllib import request as rq
import pymysql

H = os.environ.get("UCA_DB_HOST") or os.environ.get("ARMAC_DB_HOST")
U = os.environ.get("UCA_DB_USER") or os.environ.get("ARMAC_DB_USER")
P = os.environ.get("UCA_DB_PASSWORD") or os.environ.get("ARMAC_DB_PASSWORD")
PORT = int(os.environ.get("UCA_DB_PORT") or os.environ.get("ARMAC_DB_PORT") or 3306)
BASE = (os.environ.get("APP_BASE_URL") or "https://gestaofilialbh.lovable.app").rstrip("/")
SECRET = os.environ.get("SYNC_WEBHOOK_SECRET")
if not (H and U and P and SECRET):
    print("ERRO: secrets ausentes", file=sys.stderr); sys.exit(2)

QUERY = """
SELECT e.id_ordem, a.numero_armac, e.status_resultante, e.criado_em
FROM app_mov_ativos.mov_atv_evento e
JOIN app_mov_ativos.mov_atv_ordem o ON o.id_ordem = e.id_ordem
CROSS JOIN JSON_TABLE(JSON_EXTRACT(e.payload, '$.ativos'), '$[*]'
     COLUMNS (numero_armac VARCHAR(64) PATH '$.numero_armac', id_card VARCHAR(64) PATH '$.id_card')) a
WHERE e.criado_em >= UTC_TIMESTAMP() - INTERVAL 2 DAY
  AND e.status_resultante IN ('DESCARREGADO','CARREGADO')
  AND JSON_CONTAINS_PATH(e.payload, 'one', '$.ativos')
  AND (o.cod_centro_real = 'C006'
       OR (e.lat IS NOT NULL AND 111.0 * SQRT(POW(-19.9961 - e.lat, 2)
           + POW((-44.0368 - e.lon) * COS(RADIANS(e.lat)), 2)) <= 15))
ORDER BY e.criado_em
"""

con = pymysql.connect(host=H, port=PORT, user=U, password=P, connect_timeout=20, read_timeout=120)
with con.cursor() as cur:
    cur.execute(QUERY)
    rows = [{
        "id_ordem": r[0], "n_armac": (r[1] or "").strip().upper(),
        "sentido": "entrada" if r[2] == "DESCARREGADO" else "saida",
        "em": r[3].strftime("%Y-%m-%dT%H:%M:%SZ"),
    } for r in cur.fetchall() if r[1]]
con.close()
print(f"{len(rows)} movimentos de BH")
req = rq.Request(f"{BASE}/api/public/hooks/sync-app-mov", data=json.dumps({"rows": rows}).encode(),
                 headers={"Content-Type": "application/json", "x-webhook-secret": SECRET}, method="POST")
with rq.urlopen(req, timeout=120) as resp:
    print(resp.status, resp.read().decode()[:500])
