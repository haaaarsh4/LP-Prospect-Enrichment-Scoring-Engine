import sqlite3
import json
from typing import Optional
from dataclasses import asdict
from model import EnrichmentResult, ScoredProspect
from config import DB_PATH
import hashlib

def init_db(db_path: str = DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS org_enrichment (
        org_key          TEXT PRIMARY KEY,
        organization     TEXT NOT NULL,
        org_type         TEXT,
        enrichment_json  TEXT NOT NULL,
        enriched_at      TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS scored_prospects (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        contact_name     TEXT NOT NULL,
        organization     TEXT NOT NULL,
        org_type         TEXT,
        role             TEXT,
        email            TEXT,
        region           TEXT,
        contact_status   TEXT,

        enrichment_summary TEXT,
        aum_raw          TEXT,
        is_lp            INTEGER,
        sustainability_mandate INTEGER,
        private_credit_allocation INTEGER,
        emerging_manager_program INTEGER,
        brand_recognition TEXT,
        notable_facts    TEXT,

        sector_fit_score REAL,
        sector_fit_reasoning TEXT,
        sector_fit_confidence TEXT,
        relationship_depth_score REAL,
        halo_value_score REAL,
        halo_value_reasoning TEXT,
        halo_value_confidence TEXT,
        emerging_fit_score REAL,
        emerging_fit_reasoning TEXT,
        emerging_fit_confidence TEXT,
        composite_score  REAL,
        tier             TEXT,

        check_size_low   REAL,
        check_size_high  REAL,

        tokens_input     INTEGER,
        tokens_output    INTEGER,
        cost_usd         REAL,
        enriched_at      TEXT,
        data_quality     TEXT,

        run_id           TEXT,
        created_at       TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS run_log (
        run_id           TEXT PRIMARY KEY,
        started_at       TEXT,
        finished_at      TEXT,
        total_prospects  INTEGER,
        processed        INTEGER,
        skipped_cached   INTEGER,
        total_tokens_in  INTEGER,
        total_tokens_out INTEGER,
        total_cost_usd   REAL,
        status           TEXT
    )
    """)

    conn.commit()
    return conn


def org_key(organization: str) -> str:
    return hashlib.md5(organization.strip().lower().encode()).hexdigest()


def get_cached_enrichment(conn, organization: str) -> Optional[EnrichmentResult]:
    key = org_key(organization)
    row = conn.execute(
        "SELECT enrichment_json FROM org_enrichment WHERE org_key = ?", (key,)
    ).fetchone()
    if row:
        data = json.loads(row["enrichment_json"])
        return EnrichmentResult(**data)
    return None


def cache_enrichment(conn, result: EnrichmentResult):
    key = org_key(result.organization)
    conn.execute(
        """INSERT OR REPLACE INTO org_enrichment
           (org_key, organization, org_type, enrichment_json, enriched_at)
           VALUES (?, ?, ?, ?, ?)""",
        (key, result.organization, result.org_type,
         json.dumps(asdict(result)), result.enriched_at),
    )
    conn.commit()


def upsert_scored_prospect(conn, sp: ScoredProspect, run_id: str):
    # Remove existing for this contact+org (re-run support)
    conn.execute(
        "DELETE FROM scored_prospects WHERE contact_name=? AND organization=?",
        (sp.contact_name, sp.organization),
    )
    d = asdict(sp)
    d["is_lp"] = int(d["is_lp"])
    d["sustainability_mandate"] = int(d["sustainability_mandate"])
    d["private_credit_allocation"] = int(d["private_credit_allocation"])
    d["emerging_manager_program"] = int(d["emerging_manager_program"])
    d["run_id"] = run_id
    cols = list(d.keys()) + ["run_id"]
    vals = [d[c] if c != "run_id" else run_id for c in list(d.keys())] + [run_id]
    placeholders = ", ".join(["?" for _ in cols])
    col_str = ", ".join(cols)
    conn.execute(
        f"INSERT INTO scored_prospects ({col_str}) VALUES ({placeholders})", vals
    )
    conn.commit()