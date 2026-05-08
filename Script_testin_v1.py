from azure.identity import InteractiveBrowserCredential
import requests
import pandas as pd

dataset_id = "246442d4-1732-43b2-8d26-2673f473efd4"

print("Iniciando conexión con Power BI...")

credential = InteractiveBrowserCredential()
token = credential.get_token("https://analysis.windows.net/powerbi/api/.default").token

url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries"
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# ── Query 1: nivel pedido (todos los pedidos, sin filtrar) ──────────────────
dax_pedidos = """
EVALUATE
ADDCOLUMNS(
    DISTINCT(SELECTCOLUMNS(
        'LINEA_MAYOREO_DETALLADO',
        "EMPRESA",               'LINEA_MAYOREO_DETALLADO'[EMPRESA],
        "RAZON SOCIAL",          'LINEA_MAYOREO_DETALLADO'[RAZON SOCIAL],
        "NUMERO PEDIDO",         'LINEA_MAYOREO_DETALLADO'[NUMERO PEDIDO],
        "ESTADO DEL DOCUMENTO",  'LINEA_MAYOREO_DETALLADO'[ESTADO DEL DOCUMENTO],
        "NUMERO FACTURA",        'LINEA_MAYOREO_DETALLADO'[NUMERO FACTURA],
        "FECHA PEDIDO",          'LINEA_MAYOREO_DETALLADO'[FECHA PEDIDO],
        "FECHA LIBERACION",      'LINEA_MAYOREO_DETALLADO'[FECHA LIBERACION],
        "FECHA FACTURACION",     'LINEA_MAYOREO_DETALLADO'[FECHA FACTURACION],
        "FECHA LIQUIDACION",     'LINEA_MAYOREO_DETALLADO'[FECHA LIQUIDACION],
        "FECHA SALIDA",          'LINEA_MAYOREO_DETALLADO'[FECHA SALIDA],
        "FECHA_LLEGADA_CLIENTE", 'LINEA_MAYOREO_DETALLADO'[FECHA_LLEGADA_CLIENTE],
        "Conductor",             'LINEA_MAYOREO_DETALLADO'[Conductor],
        "Transporte",            'LINEA_MAYOREO_DETALLADO'[Transporte],
        "NUMERO VIAJE",          'LINEA_MAYOREO_DETALLADO'[NUMERO VIAJE],
        "CIUDAD",                'LINEA_MAYOREO_DETALLADO'[CIUDAD],
        "ESTADO",                'LINEA_MAYOREO_DETALLADO'[ESTADO],
        "ZONA DE VENTA",         'LINEA_MAYOREO_DETALLADO'[ZONA DE VENTA]
    )),
    "MONTO TOTAL", CALCULATE(IF(ISBLANK('_Medidas'[MONTO TOTAL]), 0, '_Medidas'[MONTO TOTAL]))
)
"""

# ── Query 2: nivel línea (artículos por pedido) ─────────────────────────────
dax_lineas = """
EVALUATE
SUMMARIZECOLUMNS(
    'LINEA_MAYOREO_DETALLADO'[NUMERO PEDIDO],
    'LINEA_MAYOREO_DETALLADO'[NUMERO FACTURA],
    'LINEA_MAYOREO_DETALLADO'[COD-ART],
    'LINEA_MAYOREO_DETALLADO'[ESTATUS LINEA],
    'LINEA_MAYOREO_DETALLADO'[ESTATUS LINEA FDV],
    "CANT PEDIDA",   SUM('LINEA_MAYOREO_DETALLADO'[CANT-PEDIDA]),
    "CANTIDAD ART",  SUM('LINEA_MAYOREO_DETALLADO'[CANTIDAD-ART]),
    "VENTA DOLARES", SUM('LINEA_MAYOREO_DETALLADO'[VENTA DOLARES])
)
"""

def run_query(dax, label, dedup_col=None):
    print(f"\nConsultando {label}...")
    payload = {
        "queries": [{"query": dax}],
        "serializerSettings": {"includeNulls": True}
    }
    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code == 200:
        rows = resp.json()['results'][0]['tables'][0]['rows']
        if not rows:
            print(f"  ⚠ {label}: la tabla está vacía.")
            return None
        df = pd.DataFrame(rows)
        df.columns = [col.split('[')[-1].replace(']', '') for col in df.columns]
        if 'NUMERO PEDIDO2' in df.columns:
            df.rename(columns={'NUMERO PEDIDO2': 'NUMERO PEDIDO'}, inplace=True)
        # Reemplazar None/NaN con string vacío para no perder filas
        df = df.where(pd.notnull(df), "")
        if dedup_col and dedup_col in df.columns:
            antes = len(df)
            df = df.drop_duplicates(subset=[dedup_col], keep='first')
            if antes != len(df):
                print(f"  → deduplicado por '{dedup_col}': {antes} → {len(df)} filas")
        print(f"  ✓ {label}: {len(df)} filas — columnas: {list(df.columns)}")
        return df
    else:
        print(f"  ✗ Error {resp.status_code}: {resp.text[:200]}")
        return None


df_pedidos = run_query(dax_pedidos, "PEDIDOS", dedup_col="NUMERO PEDIDO")
df_lineas  = run_query(dax_lineas,  "LÍNEAS")

if df_pedidos is not None:
    df_pedidos.to_csv("datos_pedidos_web.csv", index=False)
    print(f"\n¡Listo! datos_pedidos_web.csv guardado ({len(df_pedidos)} pedidos)")

if df_lineas is not None:
    df_lineas.to_csv("datos_lineas_web.csv", index=False)
    print(f"¡Listo! datos_lineas_web.csv guardado ({len(df_lineas)} líneas)")
