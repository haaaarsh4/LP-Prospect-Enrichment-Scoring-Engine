import os
import time
import logging
import traceback
from typing import Optional
from datetime import datetime
from groq import Groq
from model import Prospect, EnrichmentResult, ScoredProspect
from db import init_db, get_cached_enrichment, cache_enrichment, upsert_scored_prospect
from enrichment_engine import (
    enrich_organization,
    compute_composite,
    classify_tier,
    estimate_check_size,
    smart_fallback_enrichment,   # ← replaces the old dumb _fallback_enrichment
)
from config import DB_PATH, DAILY_TOKEN_LIMIT
import csv

log = logging.getLogger(__name__)
PROGRESS_WIDTH = 20


def _bar(done, total):
    filled = int(PROGRESS_WIDTH * done / max(total, 1))
    return "[" + "█" * filled + "░" * (PROGRESS_WIDTH - filled) + "]"


def load_prospects(csv_path: str) -> list[Prospect]:
    prospects = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row = {k.strip(): v.strip() for k, v in row.items()}
            try:
                rd = float(
                    row.get("Relationship Depth", row.get("relationship_depth", 5.0)) or 5.0
                )
            except (ValueError, TypeError):
                rd = 5.0
            prospects.append(
                Prospect(
                    contact_name       = row.get("Contact Name",    row.get("contact_name", "")),
                    organization       = row.get("Organization",    row.get("organization", "")),
                    org_type           = row.get("Org Type",        row.get("org_type", "")),
                    role               = row.get("Role",            row.get("role", "")),
                    email              = row.get("Email",           row.get("email", "")),
                    region             = row.get("Region",          row.get("region", "")),
                    contact_status     = row.get("Contact Status",  row.get("contact_status", "")),
                    relationship_depth = rd,
                )
            )
    return prospects


def _classify_error(err_str: str) -> str:
    """Return 'daily', 'rate', or 'other'."""
    el = err_str.lower()
    if "rate_limit_exceeded" in el or "429" in err_str or "too many requests" in el:
        if "day" in el or "daily" in el or "100,000" in err_str or "per day" in el:
            return "daily"
        return "rate"
    if "daily" in el or "quota" in el:
        return "daily"
    return "other"


def run_pipeline(
    csv_path: str,
    db_path: str = DB_PATH,
    limit: Optional[int] = None,
    force_refresh: bool = False,
):
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        raise ValueError(
            "GROQ_API_KEY environment variable not set. Run: export GROQ_API_KEY=gsk_..."
        )

    # Smoke-test the key before processing anything
    print("Testing API key...")
    try:
        test_client = Groq(api_key=api_key)
        test_resp = test_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": "Reply with the single word: READY"}],
            max_tokens=5,
        )
        print(
            f"  ✓ API key valid — model responded: "
            f"{test_resp.choices[0].message.content.strip()}\n"
        )
    except Exception as e:
        print(f"\n  ✗ API KEY TEST FAILED: {type(e).__name__}: {e}")
        print("  → Get a new key at console.groq.com and re-export it.")
        raise SystemExit(1)

    client    = Groq(api_key=api_key)
    conn      = init_db(db_path)
    run_id    = datetime.utcnow().strftime("run_%Y%m%d_%H%M%S")
    prospects = load_prospects(csv_path)
    if limit:
        prospects = prospects[:limit]

    # Deduplicate: one LLM call per unique org
    org_to_prospects: dict[str, list[Prospect]] = {}
    for p in prospects:
        org_to_prospects.setdefault(p.organization, []).append(p)
    orgs = list(org_to_prospects.keys())

    print(f"[{run_id}] Loaded {len(prospects)} contacts")
    print(f"[{run_id}] Unique orgs: {len(orgs)} (deduped from {len(prospects)} contacts)\n")

    cost_tracker  = {"total_input": 0, "total_output": 0, "total_cost": 0.0}
    processed     = 0
    skipped       = 0
    daily_quota_hit = False

    conn.execute(
        """INSERT OR REPLACE INTO run_log
           (run_id, started_at, total_prospects, processed, skipped_cached,
            total_tokens_in, total_tokens_out, total_cost_usd, status)
           VALUES (?, ?, ?, 0, 0, 0, 0, 0, 'running')""",
        (run_id, datetime.utcnow().isoformat(), len(prospects)),
    )
    conn.commit()

    enrichment_cache: dict[str, EnrichmentResult] = {}

    # ── PER-ORG ENRICHMENT LOOP ──────────────────────────────────────────────
    for i, org in enumerate(orgs):
        pct = int(i / len(orgs) * 100)
        print(f"{_bar(i, len(orgs))} {i}/{len(orgs)} orgs  ({pct}%)")
        contact_names = ", ".join(p.contact_name for p in org_to_prospects[org])
        print(f"[{run_id}] ({i+1}/{len(orgs)}) Enriching: {org}  [{contact_names}]")

        rep      = org_to_prospects[org][0]   # representative contact for this org
        org_type = rep.org_type or "Unknown"

        # ── Cache check ──────────────────────────────────────────────────────
        if not force_refresh:
            cached = get_cached_enrichment(conn, org)
            if cached:
                print(
                    f"  → Cached  "
                    f"(sector={cached.sector_fit_score}  "
                    f"halo={cached.halo_value_score}  "
                    f"emerging={cached.emerging_fit_score})\n"
                )
                enrichment_cache[org] = cached
                skipped += 1
                continue

        # ── Daily quota already exhausted → use smart defaults ────────────
        if daily_quota_hit:
            fb = smart_fallback_enrichment(
                org, org_type,
                relationship_depth=rep.relationship_depth,
                reason="daily token quota exhausted",
            )
            enrichment_cache[org] = fb
            print(
                f"  ⚠ Quota fallback  "
                f"(sector={fb.sector_fit_score}  "
                f"halo={fb.halo_value_score}  "
                f"emerging={fb.emerging_fit_score})\n"
            )
            continue

        # ── LLM enrichment with retry/backoff ────────────────────────────
        result        = None
        max_api_tries = 6   # total API-call attempts for rate-limit retries

        for attempt in range(1, max_api_tries + 1):
            try:
                result = enrich_organization(client, rep, cost_tracker)
                break   # success

            except Exception as api_err:
                err_str  = str(api_err)
                err_type = _classify_error(err_str)

                print(
                    f"  ✗ Attempt {attempt}/{max_api_tries} failed "
                    f"[{err_type}]: {err_str[:250]}"
                )

                if err_type == "daily":
                    daily_quota_hit = True
                    remaining = len(orgs) - i - 1
                    print(
                        f"\n  ✗ DAILY TOKEN QUOTA EXHAUSTED\n"
                        f"    {remaining} remaining orgs will use smart org-type defaults.\n"
                        f"    Get a new free key at console.groq.com and re-run.\n"
                    )
                    # Apply smart fallback for THIS org and break
                    result = None
                    break

                if err_type == "rate" and attempt < max_api_tries:
                    wait = min(2 ** attempt, 60)   # 2 → 4 → 8 → 16 → 32 → 60 s
                    print(f"  ⏳ Rate limit — waiting {wait}s …")
                    time.sleep(wait)
                    continue

                # Unknown error
                if attempt < max_api_tries:
                    print(f"  ↻ Retrying in 5s …\n{traceback.format_exc()}")
                    time.sleep(5)
                    continue

                print(f"  ✗ All {max_api_tries} attempts failed. Using smart defaults.\n")
                break   # will fall through to smart fallback below

        # ── Store result or apply smart fallback ─────────────────────────
        if result is not None:
            cache_enrichment(conn, result)
            enrichment_cache[org] = result
            processed += 1
            print(
                f"  ✓ sector={result.sector_fit_score:.1f}  "
                f"halo={result.halo_value_score:.1f}  "
                f"emerging={result.emerging_fit_score:.1f}  "
                f"quality={result.data_quality}\n"
            )
            # 3s between successful API calls ≈ 20 req/min (under Groq's 30/min cap)
            time.sleep(3)
        else:
            # Smart fallback — uses org-type defaults, never blanket 5.0s
            fb = smart_fallback_enrichment(
                org, org_type,
                relationship_depth=rep.relationship_depth,
                reason="LLM enrichment failed",
            )
            cache_enrichment(conn, fb)          # cache so we don't retry unnecessarily
            enrichment_cache[org] = fb
            print(
                f"  ⚠ Smart defaults  "
                f"(sector={fb.sector_fit_score}  "
                f"halo={fb.halo_value_score}  "
                f"emerging={fb.emerging_fit_score})\n"
            )

    # ── SCORING LOOP ─────────────────────────────────────────────────────────
    print(f"[{run_id}] Scoring {len(prospects)} contacts …")
    for idx, p in enumerate(prospects):
        if (idx + 1) % 10 == 0 or idx + 1 == len(prospects):
            pct = int((idx + 1) / len(prospects) * 100)
            print(
                f"  {_bar(idx + 1, len(prospects))} "
                f"{idx+1}/{len(prospects)} contacts scored  ({pct}%)"
            )

        er = enrichment_cache.get(p.organization)
        if not er:
            log.warning(f"No enrichment for {p.organization}, using smart defaults for {p.contact_name}")
            er = smart_fallback_enrichment(
                p.organization, p.org_type or "Unknown",
                relationship_depth=p.relationship_depth,
                reason="missing from enrichment cache",
            )

        composite    = compute_composite(
            er.sector_fit_score, p.relationship_depth,
            er.halo_value_score, er.emerging_fit_score,
        )
        tier         = classify_tier(composite)
        check_low, check_high = estimate_check_size(er.aum_usd, p.org_type)

        sp = ScoredProspect(
            contact_name              = p.contact_name,
            organization              = p.organization,
            org_type                  = p.org_type,
            role                      = p.role,
            email                     = p.email,
            region                    = p.region,
            contact_status            = p.contact_status,
            enrichment_summary        = er.enrichment_summary,
            aum_raw                   = er.aum_raw,
            is_lp                     = er.is_lp,
            sustainability_mandate    = er.sustainability_mandate,
            private_credit_allocation = er.private_credit_allocation,
            emerging_manager_program  = er.emerging_manager_program,
            brand_recognition         = er.brand_recognition,
            notable_facts             = er.notable_facts,
            sector_fit_score          = er.sector_fit_score,
            sector_fit_reasoning      = er.sector_fit_reasoning,
            sector_fit_confidence     = er.sector_fit_confidence,
            relationship_depth_score  = p.relationship_depth,
            halo_value_score          = er.halo_value_score,
            halo_value_reasoning      = er.halo_value_reasoning,
            halo_value_confidence     = er.halo_value_confidence,
            emerging_fit_score        = er.emerging_fit_score,
            emerging_fit_reasoning    = er.emerging_fit_reasoning,
            emerging_fit_confidence   = er.emerging_fit_confidence,
            composite_score           = round(composite, 2),
            tier                      = tier,
            check_size_low            = round(check_low)  if check_low  else None,
            check_size_high           = round(check_high) if check_high else None,
            tokens_input              = er.tokens_input,
            tokens_output             = er.tokens_output,
            cost_usd                  = er.cost_usd,
            enriched_at               = er.enriched_at,
            data_quality              = er.data_quality,
        )
        upsert_scored_prospect(conn, sp, run_id)

    # ── FINALISE ─────────────────────────────────────────────────────────────
    conn.execute(
        """UPDATE run_log SET
           finished_at=?, processed=?, skipped_cached=?,
           total_tokens_in=?, total_tokens_out=?, total_cost_usd=?, status='complete'
           WHERE run_id=?""",
        (
            datetime.utcnow().isoformat(),
            processed, skipped,
            cost_tracker["total_input"],
            cost_tracker["total_output"],
            cost_tracker["total_cost"],
            run_id,
        ),
    )
    conn.commit()
    conn.close()

    tokens_used = cost_tracker["total_input"] + cost_tracker["total_output"]
    print(f"\n{'='*54}")
    print(f"  Run {run_id} complete")
    print(f"  Orgs enriched (LLM) : {processed}   Cached : {skipped}")
    print(f"  Contacts scored     : {len(prospects)}")
    print(f"  Tokens used         : {tokens_used:,} / {DAILY_TOKEN_LIMIT:,} daily limit")
    print(f"  Cost                : $0.00 (Groq free tier)")
    print(f"{'='*54}\n")

    return run_id, cost_tracker