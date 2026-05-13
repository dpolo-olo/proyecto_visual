from flask import Flask, render_template, jsonify, send_from_directory, request
import os
import time

app = Flask(__name__)

# ── Supabase config ───────────────────────────────────────────────────────────
SUPABASE_URL      = os.environ.get("SUPABASE_URL",      "https://ttsfodcmrzqaoisikdzn.supabase.co")
SUPABASE_KEY      = os.environ.get("SUPABASE_KEY",      "")   # service_role (backend only)
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")   # publishable   (frontend)
INGEST_SECRET     = os.environ.get("INGEST_SECRET",     "")

# ── Súper-admins: acceso total a las 3 empresas ───────────────────────────────
SUPER_ADMINS = {
    'dpolo@ologistics.com',
    'jpalencia@ologistics.com',
    'wgonzalez@mayoreo.biz',
}

# ── Supabase client (para upserts complejos) ──────────────────────────────────
_sb = None
def get_supabase():
    global _sb
    if _sb: return _sb
    if not SUPABASE_KEY: return None
    try:
        from supabase import create_client
        _sb = create_client(SUPABASE_URL, SUPABASE_KEY)
        return _sb
    except Exception as e:
        print(f"[Supabase] Error: {e}")
        return None

# ── Mapeo columnas CSV <-> Supabase ───────────────────────────────────────────
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
    'VendedorKey':          'vendedor_key',
}
DB_TO_CSV = {v: k for k, v in CSV_TO_DB.items()}

# ── Fetch paginado de Supabase ────────────────────────────────────────────────
def sb_fetch_all(table, filters=""):
    """Trae TODOS los registros paginando de 1000 en 1000."""
    import requests as req
    all_rows = []
    page = 0
    page_size = 1000
    while True:
        start = page * page_size
        end   = start + page_size - 1
        r = req.get(
            f"{SUPABASE_URL}/rest/v1/{table}?select=*{filters}",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Range": f"{start}-{end}",
                "Range-Unit": "items",
                "Prefer": "count=none",
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
    return {csv_col: row.get(db_col, '') for db_col, csv_col in DB_TO_CSV.items()}

def db_linea_to_csv(row):
    return {
        'NUMERO PEDIDO':     row.get('numero_pedido', ''),
        'COD-ART':           row.get('cod_art', ''),
        'CANT PEDIDA':       row.get('cant_pedida', 0),
        'CANTIDAD ART':      row.get('cantidad_art', 0),
        'ESTATUS LINEA':     row.get('estatus_linea', ''),
        'ESTATUS LINEA FDV': row.get('estatus_fdv', ''),
        'VENTA DOLARES':     row.get('venta_dolares', 0),
    }

# ── Auth helpers ──────────────────────────────────────────────────────────────
def get_email_from_token(token):
    """Verifica el JWT de Supabase Auth y retorna el email."""
    if not token or not SUPABASE_KEY:
        return None
    try:
        import requests as req
        r = req.get(
            f"{SUPABASE_URL}/auth/v1/user",
            headers={
                "apikey":        SUPABASE_KEY,
                "Authorization": f"Bearer {token}",
            },
            timeout=10
        )
        if r.status_code == 200:
            return (r.json().get('email') or '').lower().strip()
    except Exception as e:
        print(f"[Auth] Error verificando token: {e}")
    return None

# Cache de app_users (se refresca cada 5 minutos)
_app_users_cache    = None
_app_users_cache_ts = 0
APP_USERS_TTL       = 300

def get_app_users():
    global _app_users_cache, _app_users_cache_ts
    now = time.time()
    if _app_users_cache is not None and now - _app_users_cache_ts < APP_USERS_TTL:
        return _app_users_cache
    try:
        rows = sb_fetch_all("app_users")
        _app_users_cache    = rows
        _app_users_cache_ts = now
        print(f"[app_users] Cache: {len(rows)} usuarios")
        return rows
    except Exception as e:
        print(f"[app_users] Error: {e}")
        return _app_users_cache or []

def get_user_info(email):
    """
    Determina rol y vendedores permitidos para un email.
    role:      'superadmin' | 'admin' | 'supervisor' | 'vendedor'
    vendedores: None = todos | list = VendedorKeys permitidos
    """
    email_lc = email.lower().strip()

    # ── Súper-admin ───────────────────────────────────────────────────────────
    if email_lc in SUPER_ADMINS:
        return {
            'role':      'superadmin',
            'vendedores': None,
            'nombre':    email_lc.split('@')[0].replace('.', ' ').title(),
            'empresa':   'Mayoreo',
            'email':     email_lc,
        }

    rows = get_app_users()
    if not rows:
        return None

    def match(r, field):
        return (r.get(field) or '').lower().strip() == email_lc

    # ── Admin de Ventas (correo_opventas) ─────────────────────────────────────
    admin_rows = [r for r in rows if match(r, 'correo_opventas')]
    if admin_rows:
        vkeys = list({r['vendedor_key'] for r in admin_rows if r.get('vendedor_key')})
        return {
            'role':      'admin',
            'vendedores': vkeys,
            'nombre':    admin_rows[0].get('administrador_ventas', email_lc),
            'empresa':   admin_rows[0].get('empresa', ''),
            'email':     email_lc,
        }

    # ── Supervisor Comercial (correo_supervisor) ──────────────────────────────
    sup_rows = [r for r in rows if match(r, 'correo_supervisor')]
    if sup_rows:
        vkeys = list({r['vendedor_key'] for r in sup_rows if r.get('vendedor_key')})
        return {
            'role':      'supervisor',
            'vendedores': vkeys,
            'nombre':    sup_rows[0].get('supervisor_comercial', email_lc),
            'empresa':   sup_rows[0].get('empresa', ''),
            'email':     email_lc,
        }

    # ── Vendedor (username) ───────────────────────────────────────────────────
    vend_rows = [r for r in rows if match(r, 'username')]
    if vend_rows:
        vkeys = list({r['vendedor_key'] for r in vend_rows if r.get('vendedor_key')})
        return {
            'role':      'vendedor',
            'vendedores': vkeys,
            'nombre':    vend_rows[0].get('nombre_vendedor', email_lc),
            'empresa':   vend_rows[0].get('empresa', ''),
            'email':     email_lc,
        }

    return None

def auth_required():
    """Extrae Bearer token y retorna user_info o None."""
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return None
    token = auth[7:].strip()
    email = get_email_from_token(token)
    if not email:
        return None
    return get_user_info(email)

def build_vendor_filter(user_info):
    """Construye el filtro PostgREST para vendedor_key."""
    vkeys = user_info.get('vendedores')
    if vkeys is None:          return ""                    # superadmin = sin filtro
    if len(vkeys) == 0:        return "&vendedor_key=eq.__none__"
    if len(vkeys) == 1:        return f"&vendedor_key=eq.{vkeys[0]}"
    return f"&vendedor_key=in.({','.join(vkeys)})"

# ── Rutas ─────────────────────────────────────────────────────────────────────
@app.route("/logos/<filename>")
def logos(filename):
    return send_from_directory(
        os.path.join(os.path.dirname(__file__), "templates", "logos"), filename)

@app.route("/favicon.ico")
def favicon():
    return send_from_directory(
        os.path.join(os.path.dirname(__file__), "templates", "logos"),
        "Mayoreo.png", mimetype="image/png")

@app.route("/")
def index():
    return render_template("order_tracking_v6_dateranges.html",
                           supabase_url=SUPABASE_URL,
                           supabase_anon_key=SUPABASE_ANON_KEY)

@app.route("/login")
def login_page():
    return render_template("login.html",
                           supabase_url=SUPABASE_URL,
                           supabase_anon_key=SUPABASE_ANON_KEY)

# ── API: /api/me ──────────────────────────────────────────────────────────────
@app.route("/api/me")
def api_me():
    user_info = auth_required()
    if not user_info:
        return jsonify({"error": "No autorizado"}), 401
    return jsonify({
        "email":     user_info['email'],
        "role":      user_info['role'],
        "nombre":    user_info['nombre'],
        "empresa":   user_info['empresa'],
        "vendedores": user_info['vendedores'],
    })

# ── API: /api/pedidos ─────────────────────────────────────────────────────────
@app.route("/api/pedidos")
def api_pedidos():
    if not SUPABASE_KEY:
        return jsonify({"error": "Supabase no configurado"}), 500
    user_info = auth_required()
    if not user_info:
        return jsonify({"error": "No autorizado"}), 401
    try:
        filters = build_vendor_filter(user_info)
        rows    = sb_fetch_all("pedidos", filters)
        print(f"[/api/pedidos] {len(rows)} pedidos → {user_info['email']} ({user_info['role']})")
        return jsonify([db_pedido_to_csv(r) for r in rows])
    except Exception as e:
        print(f"[/api/pedidos] Error: {e}")
        return jsonify({"error": str(e)}), 500

# ── API: /api/lineas ──────────────────────────────────────────────────────────
@app.route("/api/lineas")
def api_lineas():
    if not SUPABASE_KEY:
        return jsonify({"error": "Supabase no configurado"}), 500
    user_info = auth_required()
    if not user_info:
        return jsonify({"error": "No autorizado"}), 401
    try:
        filters = build_vendor_filter(user_info)
        rows    = sb_fetch_all("lineas", filters)
        print(f"[/api/lineas] {len(rows)} lineas → {user_info['email']} ({user_info['role']})")
        return jsonify([db_linea_to_csv(r) for r in rows])
    except Exception as e:
        print(f"[/api/lineas] Error: {e}")
        return jsonify({"error": str(e)}), 500

# ── API: /api/ingest ──────────────────────────────────────────────────────────
@app.route("/api/ingest", methods=["POST"])
def api_ingest():
    if INGEST_SECRET and request.headers.get("X-API-Key") != INGEST_SECRET:
        return jsonify({"error": "No autorizado"}), 401

    sb = get_supabase()
    if not sb:
        return jsonify({"error": "Supabase no configurado"}), 500

    data = request.get_json(force=True)
    if not data:
        return jsonify({"error": "Sin datos en el body"}), 400

    ingested = {}

    # ── Pedidos ───────────────────────────────────────────────────────────────
    if "pedidos" in data:
        rows = []
        for raw in data["pedidos"]:
            row    = {k.split('[')[-1].rstrip(']'): v for k, v in raw.items()}
            db_row = {}
            for csv_col, db_col in CSV_TO_DB.items():
                val = row.get(csv_col)
                db_row[db_col] = val if val not in ('', None) else None
            if db_row.get('id'):
                rows.append(db_row)
        if rows:
            seen = {}
            for r in rows:
                seen[r['id']] = r
            rows = list(seen.values())
            sb.table("pedidos").upsert(rows, on_conflict="id").execute()
            ingested["pedidos"] = len(rows)
            print(f"[/api/ingest] {len(rows)} pedidos actualizados")

    # ── Lineas ────────────────────────────────────────────────────────────────
    if "lineas" in data:
        # Mapa pedido → vendedor_key a partir de los pedidos del mismo payload
        pedido_vkey = {}
        if "pedidos" in data:
            for raw in data["pedidos"]:
                row  = {k.split('[')[-1].rstrip(']'): v for k, v in raw.items()}
                pid  = str(row.get('NUMERO PEDIDO') or '').strip()
                vkey = str(row.get('VendedorKey')   or '').strip()
                if pid and vkey:
                    pedido_vkey[pid] = vkey

        rows = []
        for raw in data["lineas"]:
            row = {k.split('[')[-1].rstrip(']'): v for k, v in raw.items()}
            pid = str(row.get('NUMERO PEDIDO') or '').strip()
            if not pid:
                continue
            rows.append({
                'numero_pedido': pid,
                'cod_art':       str(row.get('COD-ART',           '') or ''),
                'cant_pedida':   float(row.get('CANT PEDIDA')     or 0),
                'cantidad_art':  float(row.get('CANTIDAD ART')    or 0),
                'estatus_linea': str(row.get('ESTATUS LINEA',     '') or ''),
                'estatus_fdv':   str(row.get('ESTATUS LINEA FDV', '') or ''),
                'venta_dolares': float(row.get('VENTA DOLARES')   or 0),
                'vendedor_key':  pedido_vkey.get(pid, ''),
            })
        if rows:
            import requests as req, json as _json
            hdrs = {
                "apikey":        SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type":  "application/json",
                "Prefer":        "return=minimal",
            }
            req.delete(f"{SUPABASE_URL}/rest/v1/lineas?id=gte.1", headers=hdrs, timeout=60)
            for i in range(0, len(rows), 1000):
                req.post(f"{SUPABASE_URL}/rest/v1/lineas",
                         headers=hdrs,
                         data=_json.dumps(rows[i:i+1000], default=str),
                         timeout=60)
            ingested["lineas"] = len(rows)
            print(f"[/api/ingest] {len(rows)} lineas actualizadas")

    # ── AppUsers ──────────────────────────────────────────────────────────────
    if "appusers" in data:
        global _app_users_cache, _app_users_cache_ts
        rows = []
        for raw in data["appusers"]:
            row  = {k.split('[')[-1].rstrip(']'): v for k, v in raw.items()}
            vkey = str(row.get('VendedorKey') or '').strip()
            if not vkey:
                continue
            rows.append({
                'empresa':              str(row.get('EMPRESA')                  or '').strip(),
                'region':               str(row.get('Región')                   or '').strip(),
                'coordinacion':         str(row.get('Coordinación')             or '').strip(),
                'vendedor_key':         vkey,
                'nombre_vendedor':      str(row.get('Representante de Ventas')  or '').strip(),
                'username':             str(row.get('Username')                 or '').lower().strip(),
                'supervisor_comercial': str(row.get('Supervisor Comercial')     or '').strip(),
                'correo_supervisor':    str(row.get('Correo Supervisor')        or '').lower().strip(),
                'administrador_ventas': str(row.get('Administrador de Ventas')  or '').strip(),
                'correo_opventas':      str(row.get('Correo OpVentas')          or '').lower().strip(),
            })
        if rows:
            import requests as req, json as _json
            hdrs = {
                "apikey":        SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type":  "application/json",
                "Prefer":        "return=minimal",
            }
            req.delete(f"{SUPABASE_URL}/rest/v1/app_users?id=gte.1", headers=hdrs, timeout=30)
            req.post(f"{SUPABASE_URL}/rest/v1/app_users",
                     headers=hdrs,
                     data=_json.dumps(rows, default=str),
                     timeout=60)
            ingested["appusers"] = len(rows)
            _app_users_cache    = None   # invalidar cache
            _app_users_cache_ts = 0
            print(f"[/api/ingest] {len(rows)} app_users actualizados")

    return jsonify({"status": "ok", "ingested": ingested})

# ── Arranque ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
