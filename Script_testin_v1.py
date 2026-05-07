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

dax_query = """
EVALUATE
SUMMARIZECOLUMNS(
    'LINEA_MAYOREO_DETALLADO'[RAZON SOCIAL],
    'LINEA_MAYOREO_DETALLADO'[NUMERO PEDIDO],
    'LINEA_MAYOREO_DETALLADO'[ESTADO DEL DOCUMENTO],
    'LINEA_MAYOREO_DETALLADO'[NUMERO FACTURA],
    "MONTO TOTAL", '_Medidas'[MONTO TOTAL]
)
"""

payload = {
    "queries": [{"query": dax_query}],
    "serializerSettings": {"includeNulls": True}
}

print("Consultando los datos...")

response = requests.post(url, headers=headers, json=payload)

if response.status_code == 200:
    results = response.json()
    rows = results['results'][0]['tables'][0]['rows']
    
    if rows:
        df = pd.DataFrame(rows)
        
        df.columns = [col.split('[')[-1].replace(']', '') for col in df.columns]
        
        print("\n¡DATOS OBTENIDOS CON ÉXITO! Muestra de las primeras 5 filas:")
        print(df.head())
        
        df.to_csv("datos_pedidos_web.csv", index=False)
        print("\n¡Listo! El archivo 'datos_pedidos_web.csv' se ha creado en tu carpeta.")
    else:
        print("La consulta se ejecutó, pero la tabla está vacía.")
else:
    print(f"Error {response.status_code}: No se pudo conectar.")
    print(response.text)