from flask import Flask, render_template, jsonify, send_from_directory
import pandas as pd
import os

app = Flask(__name__)


def find_csv(filename):
    """Recorre dirs hacia arriba y devuelve el CSV más reciente encontrado."""
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


@app.route("/logos/<filename>")
def logos(filename):
    return send_from_directory(
        os.path.join(os.path.dirname(__file__), "templates", "logos"), filename
    )


@app.route("/")
def index():
    return render_template("order_tracking_v6_dateranges.html")


@app.route("/api/pedidos")
def api_pedidos():
    try:
        path = find_csv("datos_pedidos_web.csv")
        print(f"[/api/pedidos] {path}")
        df = pd.read_csv(path)
        df = df.fillna("")
        return jsonify(df.to_dict(orient="records"))
    except FileNotFoundError:
        return jsonify({
            "error": "CSV no encontrado. Ejecuta primero Script_testin_v1.py"
        }), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/lineas")
def api_lineas():
    try:
        path = find_csv("datos_lineas_web.csv")
        print(f"[/api/lineas] {path}")
        df = pd.read_csv(path)
        df = df.fillna("")
        return jsonify(df.to_dict(orient="records"))
    except FileNotFoundError:
        return jsonify([])   # Sin líneas aún — no es error crítico
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)
