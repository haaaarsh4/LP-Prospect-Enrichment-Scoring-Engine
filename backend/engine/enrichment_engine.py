import json
import re
from typing import Optional
from groq import Groq
from model import Prospect, EnrichmentResult
from config import WEIGHTS, ALLOCATION_PCT, GROQ_MODEL
from datetime import datetime

ORG_TYPE_DEFAULTS: dict[str, dict] = {
    "Foundation":           {"sector_fit": 6.5, "halo_value": 5.0, "emerging_fit": 6.5, "is_lp": True,  "is_gp": False},
    "Endowment":            {"sector_fit": 6.5, "halo_value": 5.5, "emerging_fit": 6.0, "is_lp": True,  "is_gp": False},
    "Pension":              {"sector_fit": 6.0, "halo_value": 5.0, "emerging_fit": 5.5, "is_lp": True,  "is_gp": False},
    "Single Family Office": {"sector_fit": 5.5, "halo_value": 4.5, "emerging_fit": 6.0, "is_lp": True,  "is_gp": False},
    "Multi-Family Office":  {"sector_fit": 6.0, "halo_value": 5.5, "emerging_fit": 6.0, "is_lp": True,  "is_gp": False},
    "Fund of Funds":        {"sector_fit": 7.0, "halo_value": 5.5, "emerging_fit": 6.5, "is_lp": True,  "is_gp": False},
    "HNWI":                 {"sector_fit": 5.5, "halo_value": 4.0, "emerging_fit": 5.5, "is_lp": True,  "is_gp": False},
    "Insurance":            {"sector_fit": 6.5, "halo_value": 5.5, "emerging_fit": 5.0, "is_lp": True,  "is_gp": False},
    "RIA":                  {"sector_fit": 2.0, "halo_value": 3.0, "emerging_fit": 2.0, "is_lp": False, "is_gp": True},
    "FIA":                  {"sector_fit": 2.0, "halo_value": 3.0, "emerging_fit": 2.0, "is_lp": False, "is_gp": True},
    "RIA/FIA":              {"sector_fit": 2.0, "halo_value": 3.0, "emerging_fit": 2.0, "is_lp": False, "is_gp": True},
    "Asset Manager":        {"sector_fit": 2.5, "halo_value": 4.0, "emerging_fit": 2.5, "is_lp": False, "is_gp": True},
    "Private Capital Firm": {"sector_fit": 2.5, "halo_value": 4.0, "emerging_fit": 2.5, "is_lp": False, "is_gp": True},
    "Wealth Manager":       {"sector_fit": 2.5, "halo_value": 3.5, "emerging_fit": 2.5, "is_lp": False, "is_gp": True},
}


def _get_type_defaults(org_type: str) -> dict:
    if not org_type:
        return {"sector_fit": 5.0, "halo_value": 4.5, "emerging_fit": 5.0, "is_lp": True, "is_gp": False}
    if org_type in ORG_TYPE_DEFAULTS:
        return ORG_TYPE_DEFAULTS[org_type]
    org_lower = org_type.lower()
    for key, vals in ORG_TYPE_DEFAULTS.items():
        if key.lower() in org_lower or org_lower in key.lower():
            return vals
    return {"sector_fit": 5.0, "halo_value": 4.5, "emerging_fit": 5.0, "is_lp": True, "is_gp": False}



SYSTEM_PROMPT = """You are a senior analyst at PaceZero Capital Partners scoring LP prospects for Fund II.
PaceZero = sustainability-focused PRIVATE CREDIT fund. Toronto. Emerging manager. Deals $3M–$20M. Focus: climate, ag, energy transition, health/education.

STEP 1 — LP vs GP:
LP (allocates INTO external funds): endowments, foundations, pensions, family offices, fund-of-funds.
NOT LP (manage others' money, not LP allocators): RIAs, advisors, lenders, brokers, wealth managers, asset managers (unless confirmed fund-of-funds arm).

STEP 2 — SCORE 3 DIMENSIONS (1–10):

SECTOR FIT (mandate match to sustainability + private credit):
  9–10 Confirmed LP + private credit allocation + ESG/sustainability mandate
  7–8  Confirmed LP + strong evidence of credit OR ESG
  6    Likely LP by org type + some ESG signal
  4–5  Likely LP but no mandate evidence
  2–3  Unclear if LP
  1    Confirmed GP/advisor/lender — NOT an LP

ORG TYPE DEFAULTS when info is limited:
  Foundation 6.5 | Endowment 6.5 | Pension 6 | SFO 5.5 | MFO 6 | FoF 7
  HNWI 5.5 | Insurance 6.5 | RIA 2 | Asset Manager 2.5 | Private Capital 2.5

HALO VALUE (brand pull for PaceZero's fundraise):
  9–10 Globally recognized (Rockefeller, major Ivy endowments, Wellcome Trust)
  7–8  Strong regional/sector brand in impact or climate circles
  6    Moderate visibility in specific networks
  4–5  Limited public profile
  2–3  Low profile / unknown

EMERGING MANAGER FIT (appetite for new managers):
  9–10 Documented emerging manager program
  7–8  Mission-aligned, known to back new managers
  6    Plausible by size/type (small endowment, mission-driven foundation)
  4–5  Large institution that typically needs track record
  2–3  Only backs established managers
  1    Would never consider an emerging manager

CALIBRATION ANCHORS (your scores MUST be consistent):
  Rockefeller Foundation $6.4B:       sector=9, halo=9, emerging=8
  Columbia University Endowment:      sector=9, halo=9, emerging=6
  PBUCC Pension ~$2B faith-based RI:  sector=8, halo=6, emerging=8
  Helmsley Charitable Trust large:    sector=8, halo=9, emerging=6
  Bessemer Trust MFO:                 sector=7, halo=7, emerging=6
  Morgan Stanley AIP fund-of-funds:   sector=8, halo=9, emerging=3
  Meridian Capital Group RIA:         sector=1, halo=3, emerging=1
  Aksia investment consultant:        sector=2, halo=4, emerging=2

CRITICAL RULES:
- NEVER give 5.0 across the board — differentiate based on evidence or org-type defaults above.
- GPs / advisors / lenders MUST score sector_fit 1–3.
- Apply org-type defaults when specific evidence is absent.
- Respond ONLY with raw JSON. No markdown fences, no text outside the JSON."""

ENRICHMENT_PROMPT_TEMPLATE = """Score this LP prospect for PaceZero Capital Partners Fund II.

Org: {organization}
Type: {org_type}
Region: {region}
Contact Role: {role}

Return ONLY the following JSON object — fill every field, no placeholders:

{{"organization":"{organization}","org_type":"{org_type}","enrichment_summary":"2–3 sentences on what this org does and its investment mandate","aum_raw":"e.g. $4.2B or unknown","aum_usd":null,"is_lp":true_or_false,"is_gp_or_service_provider":true_or_false,"external_fund_allocations":true_or_false,"sustainability_mandate":true_or_false,"private_credit_allocation":true_or_false,"emerging_manager_program":true_or_false,"brand_recognition":"global|regional|sector|local|unknown","notable_facts":"1–2 key facts relevant to a private credit emerging manager","sector_fit_score":NUMBER,"sector_fit_reasoning":"cite specific evidence or state which org-type default rule applies","sector_fit_confidence":"high|medium|low","halo_value_score":NUMBER,"halo_value_reasoning":"specific evidence","halo_value_confidence":"high|medium|low","emerging_fit_score":NUMBER,"emerging_fit_reasoning":"specific evidence","emerging_fit_confidence":"high|medium|low","data_quality":"sufficient|limited|minimal"}}"""

RESCUE_PROMPT_TEMPLATE = """Org: {organization}, Type: {org_type}, Region: {region}.

Reply ONLY with compact JSON, no extra text:
{{"organization":"{organization}","org_type":"{org_type}","enrichment_summary":"Brief mandate description.","aum_raw":"unknown","aum_usd":null,"is_lp":true,"is_gp_or_service_provider":false,"external_fund_allocations":true,"sustainability_mandate":false,"private_credit_allocation":false,"emerging_manager_program":false,"brand_recognition":"unknown","notable_facts":"","sector_fit_score":SCORE,"sector_fit_reasoning":"Org-type default applied.","sector_fit_confidence":"low","halo_value_score":SCORE,"halo_value_reasoning":"Org-type default applied.","halo_value_confidence":"low","emerging_fit_score":SCORE,"emerging_fit_reasoning":"Org-type default applied.","emerging_fit_confidence":"low","data_quality":"minimal"}}

Use these exact sector_fit/halo/emerging scores based on org type defaults:
Foundation→6.5/5.0/6.5 | Endowment→6.5/5.5/6.0 | Pension→6.0/5.0/5.5
SFO→5.5/4.5/6.0 | MFO→6.0/5.5/6.0 | FoF→7.0/5.5/6.5
HNWI→5.5/4.0/5.5 | Insurance→6.5/5.5/5.0
RIA→2.0/3.0/2.0 | Asset Manager→2.5/4.0/2.5 | Private Capital→2.5/4.0/2.5"""


ENRICHMENT_RESULT_FIELDS = {
    "organization", "org_type", "enrichment_summary", "aum_raw", "aum_usd",
    "is_lp", "is_gp_or_service_provider", "external_fund_allocations",
    "sustainability_mandate", "private_credit_allocation", "emerging_manager_program",
    "brand_recognition", "notable_facts",
    "sector_fit_score", "sector_fit_reasoning", "sector_fit_confidence",
    "halo_value_score", "halo_value_reasoning", "halo_value_confidence",
    "emerging_fit_score", "emerging_fit_reasoning", "emerging_fit_confidence",
    "data_quality", "enriched_at",
    "relationship_depth_score", "composite_score", "tier",
    "check_size_low", "check_size_high",
    "tokens_input", "tokens_output", "cost_usd",
}

ENRICHMENT_FIELD_DEFAULTS = {
    "enrichment_summary": "",
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


def parse_aum(aum_raw: str) -> Optional[float]:
    if not aum_raw or aum_raw.lower() in ("unknown", "n/a", "none", ""):
        return None
    s = aum_raw.upper().replace(",", "").replace("$", "").replace("USD", "").strip()
    match = re.search(r"([\d.]+)\s*([BMK]?)", s)
    if not match:
        return None
    num = float(match.group(1))
    suffix = match.group(2)
    if suffix == "B":   return num * 1e9
    elif suffix == "M": return num * 1e6
    elif suffix == "K": return num * 1e3
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
    raw = re.sub(r"^```(?:json)?\s*", "", raw.strip(), flags=re.IGNORECASE)
    raw = re.sub(r"\s*```$", "", raw.strip()).strip()

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    brace_start = raw.find("{")
    if brace_start == -1:
        raise ValueError("No JSON object in model response")
    depth = 0
    for i, ch in enumerate(raw[brace_start:], start=brace_start):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(raw[brace_start : i + 1])
                except json.JSONDecodeError as e:
                    raise ValueError(f"Malformed JSON: {e}") from e
    raise ValueError("Unbalanced braces in model response")


def _normalize_data(data: dict, organization: str, org_type: str) -> dict:
    data.setdefault("organization", organization)
    data.setdefault("org_type", org_type)
    data.setdefault("enriched_at", datetime.utcnow().isoformat())

    for field, default in ENRICHMENT_FIELD_DEFAULTS.items():
        if field not in data or (data[field] is None and default is not None):
            data[field] = default

    for k, v in list(data.items()):
        if isinstance(v, str) and v.upper().startswith("REPLACE"):
            data[k] = ENRICHMENT_FIELD_DEFAULTS.get(k, "")

    for field in (
        "is_lp", "is_gp_or_service_provider", "external_fund_allocations",
        "sustainability_mandate", "private_credit_allocation", "emerging_manager_program",
    ):
        v = data.get(field)
        if isinstance(v, str):
            data[field] = v.strip().lower() in ("true", "yes", "1")
        elif v is None:
            data[field] = False

    for field in ("sector_fit_score", "halo_value_score", "emerging_fit_score"):
        try:
            data[field] = max(1.0, min(10.0, float(data.get(field) or 5.0)))
        except (TypeError, ValueError):
            data[field] = 5.0

    if not data.get("aum_usd"):
        data["aum_usd"] = parse_aum(data.get("aum_raw", ""))

    return {k: v for k, v in data.items() if k in ENRICHMENT_RESULT_FIELDS}



def smart_fallback_enrichment(
    org: str,
    org_type: str,
    relationship_depth: float = 5.0,
    reason: str = "API unavailable",
) -> EnrichmentResult:
    """
    Build an EnrichmentResult using org-type score defaults instead of blanket 5.0s.
    Called when LLM enrichment is impossible (quota exhausted, persistent errors).
    """
    d = _get_type_defaults(org_type)
    sf    = d["sector_fit"]
    hv    = d["halo_value"]
    ef    = d["emerging_fit"]
    is_lp = d["is_lp"]
    is_gp = d["is_gp"]

    composite = compute_composite(sf, relationship_depth, hv, ef)
    tier      = classify_tier(composite)

    summary = (
        f"{org} is a {org_type} that likely allocates capital externally. "
        f"Scored using org-type defaults ({reason})."
        if is_lp else
        f"{org} is classified as a {org_type} and is likely a GP or service provider, "
        f"not an LP allocator. Scored using org-type defaults ({reason})."
    )

    return EnrichmentResult(
        organization=org,
        org_type=org_type,
        enrichment_summary=summary,
        aum_raw="unknown",
        aum_usd=None,
        is_lp=is_lp,
        is_gp_or_service_provider=is_gp,
        external_fund_allocations=is_lp,
        sustainability_mandate=False,
        private_credit_allocation=False,
        emerging_manager_program=False,
        brand_recognition="unknown",
        notable_facts="Scored via org-type defaults — manual review recommended.",
        sector_fit_score=sf,
        sector_fit_reasoning=f"Org-type default for {org_type}.",
        sector_fit_confidence="low",
        halo_value_score=hv,
        halo_value_reasoning=f"Org-type default for {org_type}.",
        halo_value_confidence="low",
        emerging_fit_score=ef,
        emerging_fit_reasoning=f"Org-type default for {org_type}.",
        emerging_fit_confidence="low",
        relationship_depth_score=relationship_depth,
        composite_score=round(composite, 2),
        tier=tier,
        check_size_low=None,
        check_size_high=None,
        tokens_input=0,
        tokens_output=0,
        cost_usd=0.0,
        enriched_at=datetime.utcnow().isoformat(),
        data_quality="minimal",
    )



def enrich_organization(
    client: Groq,
    prospect: Prospect,
    cost_tracker: dict,
) -> EnrichmentResult:
    """
    Call the LLM to enrich a prospect. Tries the full prompt first; on JSON
    parse failure automatically retries with the lighter rescue prompt.
    Raises on hard API errors so the caller can apply retry/backoff logic.
    """
    org      = prospect.organization
    org_type = prospect.org_type or "Unknown"
    region   = prospect.region   or "Unknown"
    role     = prospect.role     or "Unknown"

    attempts = [
        ENRICHMENT_PROMPT_TEMPLATE.format(
            organization=org, org_type=org_type, region=region, role=role
        ),
        RESCUE_PROMPT_TEMPLATE.format(
            organization=org, org_type=org_type, region=region
        ),
    ]

    last_exc  = None
    total_in  = 0
    total_out = 0

    for attempt_idx, user_prompt in enumerate(attempts, start=1):
        try:
            response = client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": user_prompt},
                ],
                temperature=0.1,
                max_tokens=700,
            )

            raw = response.choices[0].message.content.strip()
            ti  = response.usage.prompt_tokens     if response.usage else 0
            to  = response.usage.completion_tokens if response.usage else 0
            total_in  += ti
            total_out += to

            data = _extract_json(raw)
            data = _normalize_data(data, org, org_type)

            data["relationship_depth_score"] = prospect.relationship_depth
            data["tokens_input"]  = total_in
            data["tokens_output"] = total_out
            data["cost_usd"]      = 0.0

            cost_tracker["total_input"]  += total_in
            cost_tracker["total_output"] += total_out
            cost_tracker["total_cost"]   += 0.0

            return EnrichmentResult(**data)

        except (ValueError, KeyError) as parse_err:
            last_exc = parse_err
            if attempt_idx < len(attempts):
                continue
            raise ValueError(
                f"JSON parse failed after {len(attempts)} attempts for {org}: {parse_err}"
            ) from parse_err

        except Exception:
            raise

    raise RuntimeError(f"All enrichment attempts exhausted for {org}")