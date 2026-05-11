from flask import Flask, render_template, jsonify, send_from_directory, request
import pandas as pd
import os

app = Flask(__name__)

# ── Supabase config ───────────────────────────────────────────────────────────
SUPABASE_URL   = os.environ.get("SUPABASE_URL",   "https://ttsfodcmrzqaoisikdzn.supabase.co")
SUPABASE_KEY   = os.environ.get("SUPABASE_KEY",   "")   # secret key — se pone en Railway
INGEST_SECRET  = os.environ.get("INGEST_SECRET",  "")   # clave que usara Power Automate

_sb = None
def get_supabase():
    global _sb
    if _sb:
        return _sb
    if not SUPABASE_KEY:
        return None
    try:
        from supabase import create_client
        _sb = create_client(SUPABASE_URL, SUPABASE_KEY)
        return _sb
    except Exception as e:
        print(f"[Supabase] Error al conectar: {e}")
        return None

# ── Mapeo columnas CSV ↔ Supabase ─────────────────────────────────────────────
CSV_TO_DB = {
    'NUMERO PEDIDO':        'id',
    'EMPRESA':              'empresa',
    'RAZON SOCIAL':         'razon_social',
    'ESTADO DEL DOCUMENTO': 'estado',
    'MONTO TOTAL':          'monto',
    'FECHA PEDIDO':         'fecha_pedido',
    'NUMERO FACTURA':       'numero_factura',
    'ZONA DE VENTA':        'zona_venta',
    'CIUDAD':               'ciudad',
    'ESTADO':               'estado_geo',
    'Conductor':            'conductor',
    'Transporte':           'transporte',
    'NUMERO VIAJE':         'numero_viaje',
    'FECHA LIBERACION':     'fecha_liberacion',
    'FECHA FACTURACION':    'fecha_facturacion',
    'FECHA SALIDA':         'fecha_salida',
    'FECHA_LLEGADA_CLIENTE':'fecha_llegada_cliente',
}
DB_TO_CSV = {v: k for k, v in CSV_TO_DB.items()}

def sb_fetch_all(table):
    """Trae TODOS los registros de una tabla paginando de 1000 en 1000."""
    import requests as req
    all_rows = []
    page = 0
    page_size = 1000
    while True:
        start = page * page_size
        end   = start + page_size - 1
        r = req.get(
            f"{SUPABASE_URL}/rest/v1/{table}?select=*",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Range": f"{start}-{end}",
                "Range-Unit": "items",
                "Prefer": "count=none"
            },
            timeout=30
        )
        if r.status_code not in (200, 206):
            raise Exception(f"Supabase HTTP {r.status_code}: {r.text[:200]}")
        batch = r.json()
        all_rows.extend(batch)
        if len(batch) < page_size:
            break
        page += 1
    return all_rows

def db_pedido_to_csv(row):
    """Convierte fila de Supabase al formato de columnas que espera el frontend."""
    return {csv_col: row.get(db_col, '') for db_col, csv_col in DB_TO_CSV.items()}

def db_linea_to_csv(row):
    return {
        'NUMERO PEDIDO':    row.get('numero_pedido', ''),
        'COD-ART':          row.get('cod_art', ''),
        'NUMERO FACTURA':   '',
        'CANT PEDIDA':      row.get('cant_pedida', 0),
        'CANTIDAD ART':     row.get('cantidad_art', 0),
        'ESTATUS LINEA':    row.get('estatus_linea', ''),
        'ESTATUS LINEA FDV':row.get('estatus_fdv', ''),
        'VENTA DOLARES':    row.get('venta_dolares', 0),
    }

# ── CSV fallback ──────────────────────────────────────────────────────────────
def find_csv(filename):
    candidates = []
    current = os.path.dirname(os.path.abspath(__file__))
    for _ in range(6):
        candidate = os.path.join(current, filename)
        if os.path.exists(candidate):
            candidates.append(candidate)
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent
    if not candidates:
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)
    return max(candidates, key=os.path.getmtime)

# ── Rutas estáticas ───────────────────────────────────────────────────────────
@app.route("/logos/<filename>")
def logos(filename):
    return send_from_directory(
        os.path.join(os.path.dirname(__file__), "templates", "logos"), filename
    )

@app.route("/favicon.ico")
def favicon():
    return send_from_directory(
        os.path.join(os.path.dirname(__file__), "templates", "logos"),
        "Mayoreo.png", mimetype="image/png"
    )

@app.route("/")
def index():
    return render_template("order_tracking_v6_dateranges.html")

# ── API pedidos ───────────────────────────────────────────────────────────────
@app.route("/api/pedidos")
def api_pedidos():
    sb = get_supabase()
    if sb:
        try:
            rows = sb_fetch_all("pedidos")
            print(f"[/api/pedidos] {len(rows)} pedidos desde Supabase")
            return jsonify([db_pedido_to_csv(r) for r in rows])
        except Exception as e:
            print(f"[Supabase /api/pedidos] {e} — usando CSV")

    # Fallback CSV
    try:
        path = find_csv("datos_pedidos_web.csv")
        print(f"[/api/pedidos CSV] {path}")
        df = pd.read_csv(path).fillna("")
        return jsonify(df.to_dict(orient="records"))
    except FileNotFoundError:
        return jsonify({"error": "Sin datos. Ejecuta Script_testin_v1.py o configura Supabase."}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── API lineas ────────────────────────────────────────────────────────────────
@app.route("/api/lineas")
def api_lineas():
    sb = get_supabase()
    if sb:
        try:
            rows = sb_fetch_all("lineas")
            print(f"[/api/lineas] {len(rows)} lineas desde Supabase")
            return jsonify([db_linea_to_csv(r) for r in rows])
        except Exception as e:
            print(f"[Supabase /api/lineas] {e} — usando CSV")

    # Fallback CSV
    try:
        path = find_csv("datos_lineas_web.csv")
        print(f"[/api/lineas CSV] {path}")
        df = pd.read_csv(path).fillna("")
        return jsonify(df.to_dict(orient="records"))
    except FileNotFoundError:
        return jsonify([])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Endpoint de ingestión para Power Automate ─────────────────────────────────
@app.route("/api/ingest", methods=["POST"])
def api_ingest():
    # Verificar clave de acceso
    if INGEST_SECRET and request.headers.get("X-API-Key") != INGEST_SECRET:
        return jsonify({"error": "No autorizado"}), 401

    sb = get_supabase()
    if not sb:
        return jsonify({"error": "Supabase no configurado"}), 500

    data = request.get_json(force=True)
    if not data:
        return jsonify({"error": "Sin datos en el body"}), 400

    ingested = {}

    # Ingestar pedidos
    if "pedidos" in data:
        rows = []
        for row in data["pedidos"]:
            db_row = {}
            for csv_col, db_col in CSV_TO_DB.items():
                val = row.get(csv_col)
                db_row[db_col] = val if val not in ('', None) else None
            if db_row.get('id'):
                rows.append(db_row)
        if rows:
            sb.table("pedidos").upsert(rows, on_conflict="id").execute()
            ingested["pedidos"] = len(rows)
            print(f"[/api/ingest] {len(rows)} pedidos actualizados en Supabase")

    # Ingestar lineas
    if "lineas" in data:
        rows = []
        for row in data["lineas"]:
            pid = str(row.get('NUMERO PEDIDO') or '').strip()
            if not pid:
                continue
            rows.append({
                'numero_pedido': pid,
                'cod_art':       str(row.get('COD-ART', '') or ''),
                'cant_pedida':   float(row.get('CANT PEDIDA') or 0),
                'cantidad_art':  float(row.get('CANTIDAD ART') or 0),
                'estatus_linea': str(row.get('ESTATUS LINEA', '') or ''),
                'estatus_fdv':   str(row.get('ESTATUS LINEA FDV', '') or ''),
                'venta_dolares': float(row.get('VENTA DOLARES') or 0),
            })
        if rows:
            pids = list({r['numero_pedido'] for r in rows})
            sb.table("lineas").delete().in_("numero_pedido", pids).execute()
            sb.table("lineas").insert(rows).execute()
            ingested["lineas"] = len(rows)
            print(f"[/api/ingest] {len(rows)} lineas actualizadas en Supabase")

    return jsonify({"status": "ok", "ingested": ingested})

# ── Arranque ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
