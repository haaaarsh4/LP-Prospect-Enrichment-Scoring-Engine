import os
import json
import sqlite3
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

DB_PATH = os.environ.get("DB_PATH", "data/pacezero.db")

app = Flask(__name__, static_folder="../../frontend", static_url_path="")
CORS(app)


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def rows_to_dicts(rows):
    return [dict(r) for r in rows]


@app.route("/")
def index():
    return send_from_directory("../../frontend", "index.html")


@app.route("/api/prospects")
def get_prospects():
    conn = get_conn()
    tier_filter   = request.args.get("tier")
    region_filter = request.args.get("region")
    org_filter    = request.args.get("org_type")
    search        = request.args.get("search", "").strip()
    sort_by       = request.args.get("sort", "composite_score")
    sort_dir      = request.args.get("dir", "desc").upper()
    limit         = int(request.args.get("limit", 200))
    offset        = int(request.args.get("offset", 0))

    allowed_sorts = {
        "composite_score", "sector_fit_score", "relationship_depth_score",
        "halo_value_score", "emerging_fit_score", "contact_name",
        "organization", "region", "contact_status",
    }
    if sort_by not in allowed_sorts:
        sort_by = "composite_score"
    if sort_dir not in ("ASC", "DESC"):
        sort_dir = "DESC"

    conditions = []
    params = []

    if tier_filter:
        conditions.append("tier = ?")
        params.append(tier_filter)
    if region_filter:
        conditions.append("region = ?")
        params.append(region_filter)
    if org_filter:
        conditions.append("org_type = ?")
        params.append(org_filter)
    if search:
        conditions.append("(contact_name LIKE ? OR organization LIKE ? OR role LIKE ?)")
        like = f"%{search}%"
        params += [like, like, like]

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    rows = conn.execute(
        f"SELECT * FROM scored_prospects {where} "
        f"ORDER BY {sort_by} {sort_dir} LIMIT ? OFFSET ?",
        params + [limit, offset],
    ).fetchall()

    total = conn.execute(
        f"SELECT COUNT(*) FROM scored_prospects {where}", params
    ).fetchone()[0]

    conn.close()
    return jsonify({"prospects": rows_to_dicts(rows), "total": total})


@app.route("/api/prospects/<int:prospect_id>")
def get_prospect(prospect_id):
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM scored_prospects WHERE id = ?", (prospect_id,)
    ).fetchone()
    conn.close()
    if not row:
        return jsonify({"error": "Not found"}), 404
    return jsonify(dict(row))



@app.route("/api/stats")
def get_stats():
    conn = get_conn()

    total = conn.execute("SELECT COUNT(*) FROM scored_prospects").fetchone()[0]
    tier_counts = rows_to_dicts(conn.execute(
        "SELECT tier, COUNT(*) as count FROM scored_prospects GROUP BY tier"
    ).fetchall())

    avg_scores = conn.execute("""
        SELECT
          AVG(composite_score)       as avg_composite,
          AVG(sector_fit_score)      as avg_sector,
          AVG(relationship_depth_score) as avg_relationship,
          AVG(halo_value_score)      as avg_halo,
          AVG(emerging_fit_score)    as avg_emerging
        FROM scored_prospects
    """).fetchone()

    by_region = rows_to_dicts(conn.execute(
        "SELECT region, COUNT(*) as count, AVG(composite_score) as avg_score "
        "FROM scored_prospects GROUP BY region ORDER BY avg_score DESC"
    ).fetchall())

    by_org_type = rows_to_dicts(conn.execute(
        "SELECT org_type, COUNT(*) as count, AVG(composite_score) as avg_score "
        "FROM scored_prospects GROUP BY org_type ORDER BY avg_score DESC"
    ).fetchall())

    top_prospects = rows_to_dicts(conn.execute(
        "SELECT contact_name, organization, org_type, composite_score, tier "
        "FROM scored_prospects ORDER BY composite_score DESC LIMIT 10"
    ).fetchall())

    # Cost from latest run
    run_cost = conn.execute(
        "SELECT total_cost_usd, total_tokens_in, total_tokens_out, processed, "
        "skipped_cached, started_at FROM run_log ORDER BY started_at DESC LIMIT 1"
    ).fetchone()

    # Score distribution
    score_dist = rows_to_dicts(conn.execute("""
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
    """).fetchall())

    conn.close()
    return jsonify({
        "total": total,
        "tier_counts": tier_counts,
        "avg_scores": dict(avg_scores) if avg_scores else {},
        "by_region": by_region,
        "by_org_type": by_org_type,
        "top_prospects": top_prospects,
        "score_distribution": score_dist,
        "run_cost": dict(run_cost) if run_cost else {},
    })


@app.route("/api/filters")
def get_filter_options():
    conn = get_conn()
    tiers    = [r[0] for r in conn.execute("SELECT DISTINCT tier FROM scored_prospects WHERE tier IS NOT NULL ORDER BY tier").fetchall()]
    regions  = [r[0] for r in conn.execute("SELECT DISTINCT region FROM scored_prospects WHERE region IS NOT NULL ORDER BY region").fetchall()]
    org_types = [r[0] for r in conn.execute("SELECT DISTINCT org_type FROM scored_prospects WHERE org_type IS NOT NULL ORDER BY org_type").fetchall()]
    statuses = [r[0] for r in conn.execute("SELECT DISTINCT contact_status FROM scored_prospects WHERE contact_status IS NOT NULL ORDER BY contact_status").fetchall()]
    conn.close()
    return jsonify({"tiers": tiers, "regions": regions, "org_types": org_types, "statuses": statuses})


@app.route("/api/runs")
def get_runs():
    conn = get_conn()
    rows = rows_to_dicts(conn.execute(
        "SELECT * FROM run_log ORDER BY started_at DESC LIMIT 20"
    ).fetchall())
    conn.close()
    return jsonify(rows)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    print(f"\n Dashboard deplyed at http://localhost:{port}\n")
    app.run(debug=False, port=port)