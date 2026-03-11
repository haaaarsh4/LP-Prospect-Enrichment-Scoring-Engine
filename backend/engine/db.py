import os
import psycopg2
import psycopg2.extras
import json
from typing import Optional
from dataclasses import asdict
from model import EnrichmentResult, ScoredProspect
import hashlib


def get_conn():
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    psycopg2.extras.register_default_jsonb(conn)
    return conn


def init_db(db_path: str = None):
    conn = get_conn()
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
        id               SERIAL PRIMARY KEY,
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
        created_at       TEXT DEFAULT CURRENT_TIMESTAMP,

        UNIQUE(contact_name, organization)
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

    cur.execute("CREATE INDEX IF NOT EXISTS idx_sp_composite ON scored_prospects (composite_score DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sp_tier ON scored_prospects (tier)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sp_org ON scored_prospects (organization)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sp_region ON scored_prospects (region)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sp_org_type ON scored_prospects (org_type)")

    conn.commit()
    cur.close()
    return conn


def org_key(organization: str) -> str:
    return hashlib.md5(organization.strip().lower().encode()).hexdigest()


def get_cached_enrichment(conn, organization: str) -> Optional[EnrichmentResult]:
    key = org_key(organization)
    cur = conn.cursor()
    cur.execute("SELECT enrichment_json FROM org_enrichment WHERE org_key = %s", (key,))
    row = cur.fetchone()
    cur.close()
    if row:
        data = json.loads(row[0])
        return EnrichmentResult(**data)
    return None


def cache_enrichment(conn, result: EnrichmentResult):
    key = org_key(result.organization)
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO org_enrichment (org_key, organization, org_type, enrichment_json, enriched_at)
           VALUES (%s, %s, %s, %s, %s)
           ON CONFLICT (org_key) DO UPDATE SET
               organization = EXCLUDED.organization,
               org_type = EXCLUDED.org_type,
               enrichment_json = EXCLUDED.enrichment_json,
               enriched_at = EXCLUDED.enriched_at""",
        (key, result.organization, result.org_type, json.dumps(asdict(result)), result.enriched_at),
    )
    conn.commit()
    cur.close()


def upsert_scored_prospect(conn, sp: ScoredProspect, run_id: str):
    d = asdict(sp)

    for bool_field in ("is_lp", "sustainability_mandate", "private_credit_allocation", "emerging_manager_program"):
        if bool_field in d:
            d[bool_field] = int(bool(d[bool_field]))

    cols = list(d.keys()) + ["run_id"]
    vals = list(d.values()) + [run_id]

    placeholders = ", ".join(["%s"] * len(cols))
    col_str = ", ".join(cols)
    update_str = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in ("contact_name", "organization"))

    cur = conn.cursor()
    cur.execute(
        f"""INSERT INTO scored_prospects ({col_str}) VALUES ({placeholders})
            ON CONFLICT (contact_name, organization) DO UPDATE SET {update_str}""",
        vals,
    )
    conn.commit()
    cur.close()