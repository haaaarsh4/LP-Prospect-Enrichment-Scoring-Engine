import os
import json
import re
from typing import Optional
from google import genai
from google.genai import types
from model import Prospect, EnrichmentResult
from config import WEIGHTS, ALLOCATION_PCT, INPUT_TOKEN_COST_PER_M, OUTPUT_TOKEN_COST_PER_M, MODEL
from datetime import datetime

SYSTEM_PROMPT = """You are a senior analyst at a sustainability-focused private credit fund.
Your task is to research organizations that are potential Limited Partner (LP) investors
and score them as prospects for PaceZero Capital Partners.

PaceZero background:
- Strategy: Private credit / direct lending (NOT equity, NOT venture, NOT distressed)
- Focus: Climate mitigation, sustainability, agriculture, energy transition, health & education
- Fund status: Fund II fundraising, emerging manager (founded 2021, Toronto)
- Typical deal sizes: $3M–$20M
- Existing LP: The Atmospheric Fund (Toronto climate investor)
- Track record: 12 deals including MyLand, SWTCH Energy, Alchemy CO2, Kanin Energy

CRITICAL LP vs GP distinction:
- An LP allocates capital into funds managed by EXTERNAL GPs
- A GP originates loans, manages funds, or advises clients — NOT an LP
- If an organization's primary business is originating loans, brokering deals,
  managing portfolios for others, or providing financial services — it is a GP/service
  provider and should score VERY LOW (1-2) for sector fit
- Some orgs do both (e.g. a family office with internal vehicles that ALSO allocates
  externally) — if there is evidence of external fund allocations, treat as LP

For Foundations, Endowments, Pensions: research their INVESTMENT OFFICE / asset
management activities, not their charitable programs.

You must respond ONLY with a valid JSON object matching the exact schema below.
No preamble, no markdown fences, just the raw JSON.
"""

ENRICHMENT_PROMPT_TEMPLATE = """Research this organization and produce a structured scoring assessment.

Organization: {organization}
Org Type: {org_type}
Region: {region}
Contact Role: {role}

Use your knowledge and reasoning to assess this organization. Be specific about evidence.
If you have limited information, say so explicitly — do NOT fabricate data.

Return ONLY this JSON object (fill every field, no markdown, no backticks):

{{
  "organization": "{organization}",
  "org_type": "{org_type}",
  "enrichment_summary": "2-4 sentence factual summary of what this org does, investment focus, AUM if known",
  "aum_raw": "e.g. '$4.2B' or 'unknown'",
  "aum_usd": null,
  "is_lp": true,
  "is_gp_or_service_provider": false,
  "external_fund_allocations": true,
  "sustainability_mandate": true,
  "private_credit_allocation": true,
  "emerging_manager_program": false,
  "brand_recognition": "global",
  "notable_facts": "1-2 most relevant facts for a private credit fundraiser",

  "sector_fit_score": 7.5,
  "sector_fit_reasoning": "specific evidence for this score",
  "sector_fit_confidence": "high",

  "halo_value_score": 6.0,
  "halo_value_reasoning": "specific evidence for this score",
  "halo_value_confidence": "medium",

  "emerging_fit_score": 6.5,
  "emerging_fit_reasoning": "specific evidence for this score",
  "emerging_fit_confidence": "medium",

  "data_quality": "sufficient",
  "enriched_at": "{timestamp}"
}}

SCORING RUBRICS:

SECTOR FIT (1-10): Does this entity allocate capital to private credit/debt funds AND have sustainability/impact mandate?
  9-10: Both private credit allocation AND sustainability/impact mandate confirmed with evidence
  7-8:  Strong evidence of one, reasonable evidence of other
  5-6:  Likely LP, partial alignment (impact only, or credit only, not both)
  3-4:  Unclear LP status, or mandate misaligned
  1-2:  GP, service provider, lender, broker, or advisor — NOT an LP. Score 1 if primary business is loans/advisory.

HALO VALUE (1-10): Would winning this LP signal strongly to other LPs?
  9-10: Globally recognized brand (Rockefeller Foundation, major endowment, etc.)
  7-8:  Strong regional/sector brand, well-known in impact/climate circles
  5-6:  Moderate visibility, known within specific networks
  3-4:  Limited public profile
  1-2:  Unknown, or negative brand signal

EMERGING MANAGER FIT (1-10): Structural appetite for Fund I/II or early-stage GPs?
  9-10: Documented emerging manager program, multiple Fund I commitments on record
  7-8:  Smaller AUM allocator, mission-aligned, known to back new managers
  5-6:  No explicit program but org type/size makes it plausible
  3-4:  Large institution with bureaucratic processes, unlikely to deviate
  1-2:  Explicitly focuses only on established managers, very large minimums

CONFIDENCE levels: "high" (strong evidence), "medium" (reasonable inference), "low" (educated guess)
DATA QUALITY: "sufficient" (good evidence), "limited" (gaps in evidence), "minimal" (very little known)
"""

# ─── FIELDS THAT EnrichmentResult EXPECTS FROM GEMINI ───────────────────────
# These are the only keys we pass to EnrichmentResult(**...).
# Any extra keys returned by Gemini are stripped before construction.
ENRICHMENT_RESULT_FIELDS = {
    "organization", "org_type", "enrichment_summary", "aum_raw", "aum_usd",
    "is_lp", "is_gp_or_service_provider", "external_fund_allocations",
    "sustainability_mandate", "private_credit_allocation", "emerging_manager_program",
    "brand_recognition", "notable_facts",
    "sector_fit_score", "sector_fit_reasoning", "sector_fit_confidence",
    "halo_value_score", "halo_value_reasoning", "halo_value_confidence",
    "emerging_fit_score", "emerging_fit_reasoning", "emerging_fit_confidence",
    "data_quality", "enriched_at",
    # set by caller, with defaults
    "relationship_depth_score", "composite_score", "tier",
    "check_size_low", "check_size_high",
    "tokens_input", "tokens_output", "cost_usd",
}

ENRICHMENT_FIELD_DEFAULTS = {
    "enrichment_summary": "No summary available.",
    "aum_raw": "unknown",
    "aum_usd": None,
    "is_lp": False,
    "is_gp_or_service_provider": False,
    "external_fund_allocations": False,
    "sustainability_mandate": False,
    "private_credit_allocation": False,
    "emerging_manager_program": False,
    "brand_recognition": "unknown",
    "notable_facts": "",
    "sector_fit_score": 5.0,
    "sector_fit_reasoning": "",
    "sector_fit_confidence": "low",
    "halo_value_score": 5.0,
    "halo_value_reasoning": "",
    "halo_value_confidence": "low",
    "emerging_fit_score": 5.0,
    "emerging_fit_reasoning": "",
    "emerging_fit_confidence": "low",
    "data_quality": "minimal",
    "relationship_depth_score": 5.0,
    "composite_score": 5.0,
    "tier": "MODERATE FIT",
    "check_size_low": None,
    "check_size_high": None,
    "tokens_input": 0,
    "tokens_output": 0,
    "cost_usd": 0.0,
}


# ─── HELPERS ─────────────────────────────────────────────────────────────────

def parse_aum(aum_raw: str) -> Optional[float]:
    if not aum_raw or aum_raw.lower() in ("unknown", "n/a", "none", ""):
        return None
    s = aum_raw.upper().replace(",", "").replace("$", "").replace("USD", "").strip()
    match = re.search(r"([\d.]+)\s*([BMK]?)", s)
    if not match:
        return None
    num = float(match.group(1))
    suffix = match.group(2)
    if suffix == "B":
        return num * 1e9
    elif suffix == "M":
        return num * 1e6
    elif suffix == "K":
        return num * 1e3
    return num


def estimate_check_size(aum_usd: Optional[float], org_type: str):
    if aum_usd is None:
        return None, None
    pct = ALLOCATION_PCT.get(org_type, 0.01)
    return aum_usd * pct * 0.5, aum_usd * pct * 1.5


def compute_composite(sector_fit, relationship_depth, halo_value, emerging_fit) -> float:
    return round(
        sector_fit         * WEIGHTS["sector_fit"] +
        relationship_depth * WEIGHTS["relationship_depth"] +
        halo_value         * WEIGHTS["halo_value"] +
        emerging_fit       * WEIGHTS["emerging_fit"],
        2,
    )


def classify_tier(composite: float) -> str:
    if composite >= 8.0:   return "PRIORITY CLOSE"
    elif composite >= 6.5: return "STRONG FIT"
    elif composite >= 5.0: return "MODERATE FIT"
    else:                  return "WEAK FIT"


def _extract_json(raw: str) -> dict:
    """
    Robustly extract a JSON object from a string that may contain:
    - Markdown fences (```json ... ```)
    - Leading/trailing prose
    - Multiple JSON objects (take the first complete one)
    """
    # Strip markdown fences
    raw = re.sub(r"^```(?:json)?\s*", "", raw.strip(), flags=re.IGNORECASE)
    raw = re.sub(r"\s*```$", "", raw.strip())
    raw = raw.strip()

    # Try direct parse first
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # Try to extract the first {...} block
    brace_start = raw.find("{")
    if brace_start == -1:
        raise ValueError("No JSON object found in response")

    depth = 0
    for i, ch in enumerate(raw[brace_start:], start=brace_start):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                candidate = raw[brace_start : i + 1]
                try:
                    return json.loads(candidate)
                except json.JSONDecodeError as e:
                    raise ValueError(f"Found JSON-like block but failed to parse: {e}") from e

    raise ValueError("Unbalanced JSON braces in response")


def _normalize_data(data: dict, organization: str, org_type: str) -> dict:
    """
    Apply defaults, normalize types, clamp scores, and strip unknown fields.
    """
    # Ensure required identity fields
    data.setdefault("organization", organization)
    data.setdefault("org_type", org_type)
    data.setdefault("enriched_at", datetime.utcnow().isoformat())

    # Apply defaults for all missing fields
    for field, default in ENRICHMENT_FIELD_DEFAULTS.items():
        if field not in data or data[field] is None and default is not None:
            data.setdefault(field, default)

    # Normalize boolean fields
    for field in ["is_lp", "is_gp_or_service_provider", "external_fund_allocations",
                  "sustainability_mandate", "private_credit_allocation", "emerging_manager_program"]:
        v = data.get(field)
        if isinstance(v, str):
            data[field] = v.strip().lower() in ("true", "yes", "1")
        elif v is None:
            data[field] = False

    # Clamp and coerce score fields
    for field in ["sector_fit_score", "halo_value_score", "emerging_fit_score"]:
        try:
            data[field] = max(1.0, min(10.0, float(data.get(field) or 5.0)))
        except (TypeError, ValueError):
            data[field] = 5.0

    # Parse AUM if numeric not provided
    if not data.get("aum_usd"):
        data["aum_usd"] = parse_aum(data.get("aum_raw", ""))

    # Strip any keys the dataclass doesn't know about
    data = {k: v for k, v in data.items() if k in ENRICHMENT_RESULT_FIELDS}

    return data


# ─── MAIN ENRICHMENT ─────────────────────────────────────────────────────────

def enrich_organization(
    client: genai.Client,
    prospect: Prospect,
    cost_tracker: dict,
) -> EnrichmentResult:

    prompt = SYSTEM_PROMPT + "\n\n" + ENRICHMENT_PROMPT_TEMPLATE.format(
        organization=prospect.organization,
        org_type=prospect.org_type or "Unknown",
        region=prospect.region or "Unknown",
        role=prospect.role or "Unknown",
        timestamp=datetime.utcnow().isoformat(),
    )

    response = client.models.generate_content(
        model=MODEL,
        contents=prompt,
        config=types.GenerateContentConfig(
            tools=[types.Tool(google_search=types.GoogleSearch())],
            temperature=0.2,
        ),
    )

    raw = response.text.strip()

    # Parse JSON robustly
    data = _extract_json(raw)

    # Normalize all fields — apply defaults, clamp, type-coerce, strip unknowns
    data = _normalize_data(data, prospect.organization, prospect.org_type or "Unknown")

    # Inject caller-side fields
    data["relationship_depth_score"] = prospect.relationship_depth
    data["tokens_input"]  = 0
    data["tokens_output"] = 0
    data["cost_usd"]      = 0.0

    # Token tracking (Gemini free tier → cost = 0)
    try:
        data["tokens_input"]  = response.usage_metadata.prompt_token_count or 0
        data["tokens_output"] = response.usage_metadata.candidates_token_count or 0
    except Exception:
        pass

    cost_tracker["total_input"]  += data["tokens_input"]
    cost_tracker["total_output"] += data["tokens_output"]
    cost_tracker["total_cost"]   += 0.0

    return EnrichmentResult(**data)