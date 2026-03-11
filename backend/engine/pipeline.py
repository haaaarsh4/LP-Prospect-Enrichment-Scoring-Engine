import os
import time
import logging
import re
from typing import Optional
from datetime import datetime
from google import genai
from model import Prospect, EnrichmentResult, ScoredProspect
from db import init_db, get_cached_enrichment, cache_enrichment, upsert_scored_prospect
from enrichment_engine import (
    enrich_organization, compute_composite, classify_tier, estimate_check_size,
    ENRICHMENT_FIELD_DEFAULTS,
)
from config import DB_PATH
import csv

log = logging.getLogger(__name__)

# ── RATE LIMIT CONFIG ─────────────────────────────────────────────────────────
# Gemini free tier: 15 req/min, 1500 req/day, 1M tokens/min
# We stay well under by enforcing a floor delay between org calls.
MIN_DELAY_BETWEEN_CALLS = 5   
MAX_RETRIES             = 4   
RETRY_BACKOFF_BASE      = 30  
# ─────────────────────────────────────────────────────────────────────────────


def _parse_retry_delay(error: Exception) -> float:
    """
    Extract the retryDelay suggested by the API from the error message.
    Falls back to RETRY_BACKOFF_BASE if not found.
    """
    msg = str(error)
    # Look for patterns like "retry in 23.87s" or "'retryDelay': '23s'"
    match = re.search(r"retry(?:Delay)?['\s:]*[\"']?([\d.]+)s", msg, re.IGNORECASE)
    if match:
        return float(match.group(1)) + 5  # add a small buffer
    # Also check for plain "Please retry in Xs"
    match = re.search(r"retry in ([\d.]+)", msg, re.IGNORECASE)
    if match:
        return float(match.group(1)) + 5
    return float(RETRY_BACKOFF_BASE)


def load_prospects(csv_path: str) -> list[Prospect]:
    prospects = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row = {k.strip(): v.strip() for k, v in row.items()}
            try:
                rd = float(row.get("Relationship Depth", row.get("relationship_depth", 5.0)) or 5.0)
            except (ValueError, TypeError):
                rd = 5.0
            prospects.append(Prospect(
                contact_name       = row.get("Contact Name", row.get("contact_name", "")),
                organization       = row.get("Organization", row.get("organization", "")),
                org_type           = row.get("Org Type", row.get("org_type", "")),
                role               = row.get("Role", row.get("role", "")),
                email              = row.get("Email", row.get("email", "")),
                region             = row.get("Region", row.get("region", "")),
                contact_status     = row.get("Contact Status", row.get("contact_status", "")),
                relationship_depth = rd,
            ))
    return prospects


def _make_fallback_enrichment(org: str, org_type: str, relationship_depth: float) -> EnrichmentResult:
    """
    Returns a safe fallback EnrichmentResult when the API call fails.
    Uses the contact's actual relationship_depth from CRM rather than a hardcoded 5.0.
    """
    return EnrichmentResult(
        organization=org,
        org_type=org_type,
        enrichment_summary="Enrichment failed — API error or no data returned.",
        aum_raw="unknown",
        aum_usd=None,
        is_lp=False,
        is_gp_or_service_provider=False,
        external_fund_allocations=False,
        sustainability_mandate=False,
        private_credit_allocation=False,
        emerging_manager_program=False,
        brand_recognition="unknown",
        notable_facts="",
        sector_fit_score=5.0,
        sector_fit_reasoning="Enrichment failed.",
        sector_fit_confidence="low",
        halo_value_score=5.0,
        halo_value_reasoning="Enrichment failed.",
        halo_value_confidence="low",
        emerging_fit_score=5.0,
        emerging_fit_reasoning="Enrichment failed.",
        emerging_fit_confidence="low",
        relationship_depth_score=relationship_depth,
        composite_score=5.0,
        tier="MODERATE FIT",
        check_size_low=None,
        check_size_high=None,
        tokens_input=0,
        tokens_output=0,
        cost_usd=0.0,
        enriched_at=datetime.utcnow().isoformat(),
        data_quality="minimal",
    )


def run_pipeline(
    csv_path: str,
    db_path: str = DB_PATH,
    limit: Optional[int] = None,
    force_refresh: bool = False,
):
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable not set")

    client = genai.Client(api_key=api_key)
    conn   = init_db(db_path)

    run_id    = datetime.utcnow().strftime("run_%Y%m%d_%H%M%S")
    prospects = load_prospects(csv_path)
    if limit:
        prospects = prospects[:limit]

    print(f"[{run_id}] Loaded {len(prospects)} contacts")

    #  ORG-LEVEL DEDUPLICATION 

    org_to_prospects: dict[str, list[Prospect]] = {}
    for p in prospects:
        org_to_prospects.setdefault(p.organization, []).append(p)

    for org in org_to_prospects:
        org_to_prospects[org].sort(key=lambda p: p.relationship_depth, reverse=True)

    orgs = list(org_to_prospects.keys())
    print(f"[{run_id}] Unique orgs: {len(orgs)} (deduped from {len(prospects)} contacts)")

    cost_tracker = {"total_input": 0, "total_output": 0, "total_cost": 0.0}
    processed = 0
    skipped   = 0

    conn.execute(
        """INSERT OR REPLACE INTO run_log
           (run_id, started_at, total_prospects, processed, skipped_cached,
            total_tokens_in, total_tokens_out, total_cost_usd, status)
           VALUES (?, ?, ?, 0, 0, 0, 0, 0, 'running')""",
        (run_id, datetime.utcnow().isoformat(), len(prospects)),
    )
    conn.commit()

    enrichment_cache: dict[str, EnrichmentResult] = {}

    for i, org in enumerate(orgs):
        representative = org_to_prospects[org][0]
        n_contacts = len(org_to_prospects[org])
        contact_names = ", ".join(p.contact_name for p in org_to_prospects[org][:3])
        extra = f" +{n_contacts - 3} more" if n_contacts > 3 else ""
        done_so_far = processed + skipped
        pct = int(done_so_far / len(orgs) * 100)
        bar_filled = int(pct / 5)
        bar = "█" * bar_filled + "░" * (20 - bar_filled)
        print(f"\n[{bar}] {done_so_far}/{len(orgs)} orgs  ({pct}%)")
        print(f"[{run_id}] ({i+1}/{len(orgs)}) Enriching: {org}  [{contact_names}{extra}]")

        # Check DB cache first (skip API if already enriched)
        if not force_refresh:
            cached = get_cached_enrichment(conn, org)
            if cached:
                cached.relationship_depth_score = representative.relationship_depth
                enrichment_cache[org] = cached
                skipped += 1
                print(f"  → Cached  (sector={cached.sector_fit_score:.1f}  halo={cached.halo_value_score:.1f}  emerging={cached.emerging_fit_score:.1f})")
                continue

        # Call Gemini with retry on transient errors
        result = None
        last_error = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                result = enrich_organization(client, representative, cost_tracker)
                break
            except Exception as e:
                last_error = e
                err_str = str(e)
                is_quota = "429" in err_str or "RESOURCE_EXHAUSTED" in err_str
                is_daily = "PerDay" in err_str or "GenerateRequestsPerDay" in err_str

                if is_daily:
                    # Daily quota exhausted — no point retrying until tomorrow
                    print(f"  ✗ Daily quota exhausted for this API key.")
                    print(f"    → Either wait until tomorrow, enable billing at ai.google.dev,")
                    print(f"      or switch to a different GEMINI_API_KEY.")
                    print(f"    → Remaining orgs will use fallback scores (5.0) for now.")
                    # Mark all remaining orgs as fallback and exit the enrichment loop
                    for remaining_org in orgs[i:]:
                        if remaining_org not in enrichment_cache:
                            rep = org_to_prospects[remaining_org][0]
                            enrichment_cache[remaining_org] = _make_fallback_enrichment(
                                remaining_org, rep.org_type or "Unknown", rep.relationship_depth
                            )
                    break

                if is_quota:
                    wait = _parse_retry_delay(e)
                    print(f"  ⏳ Rate limited (attempt {attempt}/{MAX_RETRIES}) — waiting {wait:.0f}s as requested by API...")
                else:
                    wait = attempt * 5
                    print(f"  ✗ Attempt {attempt}/{MAX_RETRIES} failed: {e}  — retrying in {wait}s")

                if attempt < MAX_RETRIES:
                    time.sleep(wait)

            if is_daily if 'is_daily' in dir() else False:
                break

        if result is None:
            print(f"  ✗ All {MAX_RETRIES} attempts failed for '{org}': {last_error}")
            result = _make_fallback_enrichment(
                org, representative.org_type or "Unknown",
                representative.relationship_depth,
            )
        else:
            cache_enrichment(conn, result)
            processed += 1
            print(
                f"  ✓ sector={result.sector_fit_score:.1f}  "
                f"halo={result.halo_value_score:.1f}  "
                f"emerging={result.emerging_fit_score:.1f}  "
                f"quality={result.data_quality}"
            )

        enrichment_cache[org] = result

        # Enforce minimum delay between calls to stay under per-minute rate limits
        time.sleep(MIN_DELAY_BETWEEN_CALLS)

    print(f"\n[{run_id}] Scoring {len(prospects)} contacts...")

    scored = 0
    for p in prospects:
        er = enrichment_cache.get(p.organization)
        if not er:
            print(f"  ⚠ No enrichment found for '{p.organization}' — skipping {p.contact_name}")
            continue

        composite = compute_composite(
            er.sector_fit_score,
            p.relationship_depth,   
            er.halo_value_score,
            er.emerging_fit_score,
        )
        tier = classify_tier(composite)
        check_low, check_high = estimate_check_size(er.aum_usd, p.org_type)

        sp = ScoredProspect(
            contact_name               = p.contact_name,
            organization               = p.organization,
            org_type                   = p.org_type,
            role                       = p.role,
            email                      = p.email,
            region                     = p.region,
            contact_status             = p.contact_status,
            enrichment_summary         = er.enrichment_summary,
            aum_raw                    = er.aum_raw,
            is_lp                      = er.is_lp,
            sustainability_mandate     = er.sustainability_mandate,
            private_credit_allocation  = er.private_credit_allocation,
            emerging_manager_program   = er.emerging_manager_program,
            brand_recognition          = er.brand_recognition,
            notable_facts              = er.notable_facts,
            sector_fit_score           = er.sector_fit_score,
            sector_fit_reasoning       = er.sector_fit_reasoning,
            sector_fit_confidence      = er.sector_fit_confidence,
            relationship_depth_score   = p.relationship_depth,  # per-contact
            halo_value_score           = er.halo_value_score,
            halo_value_reasoning       = er.halo_value_reasoning,
            halo_value_confidence      = er.halo_value_confidence,
            emerging_fit_score         = er.emerging_fit_score,
            emerging_fit_reasoning     = er.emerging_fit_reasoning,
            emerging_fit_confidence    = er.emerging_fit_confidence,
            composite_score            = round(composite, 2),
            tier                       = tier,
            check_size_low             = round(check_low)  if check_low  else None,
            check_size_high            = round(check_high) if check_high else None,
            tokens_input               = er.tokens_input,
            tokens_output              = er.tokens_output,
            cost_usd                   = er.cost_usd,
            enriched_at                = er.enriched_at,
            data_quality               = er.data_quality,
        )
        upsert_scored_prospect(conn, sp, run_id)
        scored += 1
        if scored % 10 == 0 or scored == len(prospects):
            pct = int(scored / len(prospects) * 100)
            bar_filled = int(pct / 5)
            bar = "█" * bar_filled + "░" * (20 - bar_filled)
            print(f"  [{bar}] {scored}/{len(prospects)} contacts scored  ({pct}%)")

    conn.execute(
        """UPDATE run_log
           SET finished_at=?, processed=?, skipped_cached=?,
               total_tokens_in=?, total_tokens_out=?, total_cost_usd=?, status='complete'
           WHERE run_id=?""",
        (
            datetime.utcnow().isoformat(),
            processed,
            skipped,
            cost_tracker["total_input"],
            cost_tracker["total_output"],
            cost_tracker["total_cost"],
            run_id,
        ),
    )
    conn.commit()
    conn.close()

    print(f"\n{'='*52}")
    print(f"  Run {run_id} complete")
    print(f"  Orgs enriched : {processed}  |  Cached: {skipped}")
    print(f"  Total contacts scored: {len(prospects)}")
    print(f"  Cost: $0.00 (Gemini free tier)")
    print(f"{'='*52}\n")

    return run_id, cost_tracker