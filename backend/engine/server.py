import os
import json
import threading
import tempfile
import shutil
from datetime import datetime
import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

_HERE = os.path.dirname(os.path.abspath(__file__))
_FRONTEND = os.path.normpath(os.path.join(_HERE, "..", "..", "frontend"))
if not os.path.isdir(_FRONTEND):
    _FRONTEND = os.path.normpath(os.path.join(_HERE, "frontend"))
if not os.path.isdir(_FRONTEND):
    _FRONTEND = os.path.normpath(os.path.join(os.getcwd(), "frontend"))

print(f"  Frontend dir: {_FRONTEND}  (exists={os.path.isdir(_FRONTEND)})")

app = Flask(__name__, static_folder=_FRONTEND, static_url_path="/static-assets")
CORS(app)


@app.errorhandler(404)
def not_found(e):
    if request.path.startswith("/api"):
        return jsonify({"error": f"API route not found: {request.path}"}), 404
    idx = os.path.join(_FRONTEND, "index.html")
    if os.path.isfile(idx):
        return send_from_directory(_FRONTEND, "index.html")
    return jsonify({"error": "Frontend not found"}), 404


@app.errorhandler(405)
def method_not_allowed(e):
    return jsonify({"error": f"Method not allowed: {request.method} {request.path}"}), 405


@app.errorhandler(500)
def internal_error(e):
    return jsonify({"error": f"Internal server error: {str(e)}"}), 500


_run_state = {
    "active": False,
    "run_id": None,
    "status": "idle",
    "message": "",
    "orgs_total": 0,
    "orgs_done": 0,
    "contacts_total": 0,
    "contacts_done": 0,
    "current_org": "",
    "error": None,
    "started_at": None,
    "finished_at": None,
}
_run_lock = threading.Lock()


def _update_state(**kwargs):
    with _run_lock:
        _run_state.update(kwargs)


def get_conn():
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    return conn


def rows_to_dicts(cur, rows):
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in rows]


def fetchone_dict(cur):
    row = cur.fetchone()
    if row is None:
        return None
    cols = [desc[0] for desc in cur.description]
    return dict(zip(cols, row))


@app.route("/")
def index():
    idx = os.path.join(_FRONTEND, "index.html")
    if os.path.isfile(idx):
        return send_from_directory(_FRONTEND, "index.html")
    return jsonify({"error": f"index.html not found in {_FRONTEND}"}), 404


@app.route("/<path:path>")
def spa_fallback(path):
    if path.startswith("api/"):
        return jsonify({"error": f"Unknown API endpoint: /{path}"}), 404
    full = os.path.join(_FRONTEND, path)
    if os.path.isfile(full):
        return send_from_directory(_FRONTEND, path)
    idx = os.path.join(_FRONTEND, "index.html")
    if os.path.isfile(idx):
        return send_from_directory(_FRONTEND, "index.html")
    return jsonify({"error": "Not found"}), 404


@app.route("/api/upload", methods=["POST"])
def upload_csv():
    try:
        with _run_lock:
            if _run_state["active"]:
                return jsonify({"error": "A pipeline run is already in progress.", "run_id": _run_state["run_id"]}), 409

        if "file" not in request.files:
            return jsonify({"error": "No file provided. Send multipart/form-data with field 'file'."}), 400

        f = request.files["file"]
        if not f.filename.endswith(".csv"):
            return jsonify({"error": "Only CSV files are supported."}), 400

        tmp_dir = tempfile.mkdtemp()
        csv_path = os.path.join(tmp_dir, "upload.csv")
        f.save(csv_path)

        force_refresh = request.form.get("force_refresh", "false").lower() == "true"

        _update_state(
            active=True,
            status="running",
            message="Starting pipeline...",
            run_id=None,
            orgs_total=0,
            orgs_done=0,
            contacts_total=0,
            contacts_done=0,
            current_org="",
            error=None,
            started_at=datetime.utcnow().isoformat(),
            finished_at=None,
        )

        t = threading.Thread(target=_run_pipeline_thread, args=(csv_path, tmp_dir, force_refresh), daemon=True)
        t.start()

        return jsonify({"status": "started", "message": "Pipeline started."})

    except Exception as e:
        return jsonify({"error": f"Upload failed: {str(e)}"}), 500


def _run_pipeline_thread(csv_path: str, tmp_dir: str, force_refresh: bool):
    import builtins
    import re

    original_print = builtins.print

    def patched_print(*args, **kwargs):
        original_print(*args, **kwargs)
        line = " ".join(str(a) for a in args)

        m = re.search(r"Unique orgs:\s*(\d+).*?(\d+) contacts", line)
        if m:
            _update_state(orgs_total=int(m.group(1)), contacts_total=int(m.group(2)), message=f"Enriching {m.group(1)} orgs...")

        m = re.search(r"\((\d+)/(\d+)\) Enriching: (.+?)(?:\s{2,}|\[|$)", line)
        if m:
            _update_state(orgs_done=max(0, int(m.group(1)) - 1), orgs_total=int(m.group(2)), current_org=m.group(3).strip(), message=f"Enriching: {m.group(3).strip()}")

        if "Cached" in line or line.strip().startswith("✓") or line.strip().startswith("⚠"):
            with _run_lock:
                _run_state["orgs_done"] = _run_state.get("orgs_done", 0) + 1

        m = re.search(r"\] (\d+)/(\d+) contacts scored", line)
        if m:
            _update_state(contacts_done=int(m.group(1)), contacts_total=int(m.group(2)), message=f"Scoring contacts: {m.group(1)}/{m.group(2)}")

        if "complete" in line.lower() and "run_" in line.lower():
            m = re.search(r"(run_\w+)", line)
            if m:
                _update_state(run_id=m.group(1))

    builtins.print = patched_print

    try:
        from pipeline import run_pipeline
        run_id, costs = run_pipeline(csv_path, force_refresh=force_refresh)
        _update_state(active=False, status="complete", run_id=run_id, message=f"Complete! Run {run_id} finished.", finished_at=datetime.utcnow().isoformat())
    except Exception as e:
        _update_state(active=False, status="error", error=str(e), message=f"Pipeline error: {e}", finished_at=datetime.utcnow().isoformat())
    finally:
        builtins.print = original_print
        shutil.rmtree(tmp_dir, ignore_errors=True)


@app.route("/api/run-status")
def run_status():
    with _run_lock:
        return jsonify(dict(_run_state))


@app.route("/api/prospects")
def get_prospects():
    try:
        conn = get_conn()
        cur = conn.cursor()

        tier_filter = request.args.get("tier")
        region_filter = request.args.get("region")
        org_filter = request.args.get("org_type")
        search = request.args.get("search", "").strip()
        sort_by = request.args.get("sort", "composite_score")
        sort_dir = request.args.get("dir", "desc").upper()
        limit = int(request.args.get("limit", 200))
        offset = int(request.args.get("offset", 0))

        allowed_sorts = {"composite_score", "sector_fit_score", "relationship_depth_score", "halo_value_score", "emerging_fit_score", "contact_name", "organization", "region", "contact_status"}
        if sort_by not in allowed_sorts:
            sort_by = "composite_score"
        if sort_dir not in ("ASC", "DESC"):
            sort_dir = "DESC"

        conditions, params = [], []

        if tier_filter == "FAILED":
            conditions.append("data_quality = 'minimal'")
            conditions.append("enrichment_summary LIKE '%Enrichment failed%'")
        elif tier_filter:
            conditions.append("tier = %s")
            params.append(tier_filter)

        if region_filter:
            conditions.append("region = %s")
            params.append(region_filter)
        if org_filter:
            conditions.append("org_type = %s")
            params.append(org_filter)
        if search:
            conditions.append("(contact_name ILIKE %s OR organization ILIKE %s OR role ILIKE %s)")
            like = f"%{search}%"
            params += [like, like, like]

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        cur.execute(f"SELECT * FROM scored_prospects {where} ORDER BY {sort_by} {sort_dir} LIMIT %s OFFSET %s", params + [limit, offset])
        rows = rows_to_dicts(cur, cur.fetchall())

        cur.execute(f"SELECT COUNT(*) FROM scored_prospects {where}", params)
        total = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM scored_prospects WHERE data_quality = 'minimal' AND enrichment_summary LIKE '%Enrichment failed%'")
        failed_count = cur.fetchone()[0]

        cur.close()
        conn.close()
        return jsonify({"prospects": rows, "total": total, "failed_count": failed_count})

    except Exception as e:
        return jsonify({"error": str(e), "prospects": [], "total": 0, "failed_count": 0}), 500


@app.route("/api/prospects/<int:prospect_id>")
def get_prospect(prospect_id):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM scored_prospects WHERE id = %s", (prospect_id,))
        row = fetchone_dict(cur)
        cur.close()
        conn.close()
        if not row:
            return jsonify({"error": "Not found"}), 404
        return jsonify(row)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/stats")
def get_stats():
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM scored_prospects")
        total = cur.fetchone()[0]

        cur.execute("SELECT tier, COUNT(*) as count FROM scored_prospects GROUP BY tier")
        tier_counts = rows_to_dicts(cur, cur.fetchall())

        cur.execute("""
            SELECT
              AVG(composite_score) as avg_composite,
              AVG(sector_fit_score) as avg_sector,
              AVG(relationship_depth_score) as avg_relationship,
              AVG(halo_value_score) as avg_halo,
              AVG(emerging_fit_score) as avg_emerging
            FROM scored_prospects
        """)
        avg_scores = fetchone_dict(cur) or {}

        cur.execute("SELECT region, COUNT(*) as count, AVG(composite_score) as avg_score FROM scored_prospects GROUP BY region ORDER BY avg_score DESC")
        by_region = rows_to_dicts(cur, cur.fetchall())

        cur.execute("SELECT org_type, COUNT(*) as count, AVG(composite_score) as avg_score FROM scored_prospects GROUP BY org_type ORDER BY avg_score DESC")
        by_org_type = rows_to_dicts(cur, cur.fetchall())

        cur.execute("SELECT contact_name, organization, org_type, composite_score, tier FROM scored_prospects ORDER BY composite_score DESC LIMIT 10")
        top_prospects = rows_to_dicts(cur, cur.fetchall())

        cur.execute("SELECT total_cost_usd, total_tokens_in, total_tokens_out, processed, skipped_cached, started_at FROM run_log ORDER BY started_at DESC LIMIT 1")
        run_cost = fetchone_dict(cur) or {}

        cur.execute("""
            SELECT
              CASE
                WHEN composite_score >= 9 THEN '9-10'
                WHEN composite_score >= 8 THEN '8-9'
                WHEN composite_score >= 7 THEN '7-8'
                WHEN composite_score >= 6 THEN '6-7'
                WHEN composite_score >= 5 THEN '5-6'
                ELSE '<5'
              END as bucket,
              COUNT(*) as count
            FROM scored_prospects
            GROUP BY bucket ORDER BY bucket DESC
        """)
        score_dist = rows_to_dicts(cur, cur.fetchall())

        cur.close()
        conn.close()
        return jsonify({
            "total": total,
            "tier_counts": tier_counts,
            "avg_scores": avg_scores,
            "by_region": by_region,
            "by_org_type": by_org_type,
            "top_prospects": top_prospects,
            "score_distribution": score_dist,
            "run_cost": run_cost,
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/filters")
def get_filter_options():
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("SELECT DISTINCT tier FROM scored_prospects WHERE tier IS NOT NULL ORDER BY tier")
        tiers = [r[0] for r in cur.fetchall()]

        cur.execute("SELECT DISTINCT region FROM scored_prospects WHERE region IS NOT NULL ORDER BY region")
        regions = [r[0] for r in cur.fetchall()]

        cur.execute("SELECT DISTINCT org_type FROM scored_prospects WHERE org_type IS NOT NULL ORDER BY org_type")
        org_types = [r[0] for r in cur.fetchall()]

        cur.close()
        conn.close()
        return jsonify({"tiers": tiers, "regions": regions, "org_types": org_types})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/runs")
def get_runs():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM run_log ORDER BY started_at DESC LIMIT 20")
        rows = rows_to_dicts(cur, cur.fetchall())
        cur.close()
        conn.close()
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    print(f"\n  PaceZero LP Intelligence")
    print(f"  http://localhost:{port}\n")
    app.run(debug=False, port=port, threaded=True)