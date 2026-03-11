from dataclasses import dataclass
from typing import Optional


@dataclass
# Input fields from challenge_contacts.csv
class Prospect:
    contact_name: str
    organization: str
    org_type: str
    role: str
    email: str
    region: str
    contact_status: str
    relationship_depth: float


@dataclass
class EnrichmentResult:
    organization: str
    org_type: str

    # Raw enrichment
    enrichment_summary: str
    aum_raw: str
    aum_usd: Optional[float]
    is_lp: bool
    is_gp_or_service_provider: bool
    external_fund_allocations: bool
    sustainability_mandate: bool
    private_credit_allocation: bool
    emerging_manager_program: bool
    brand_recognition: str          # global | regional | sector | local | unknown
    notable_facts: str

    # Dimension scores
    sector_fit_score: float
    sector_fit_reasoning: str
    sector_fit_confidence: str      # high | medium | low

    halo_value_score: float
    halo_value_reasoning: str
    halo_value_confidence: str

    emerging_fit_score: float
    emerging_fit_reasoning: str
    emerging_fit_confidence: str

    # Computed / passed-through
    relationship_depth_score: float
    composite_score: float
    tier: str
    check_size_low: Optional[float]
    check_size_high: Optional[float]

    # Meta
    tokens_input: int
    tokens_output: int
    cost_usd: float
    enriched_at: str
    data_quality: str               # sufficient | limited | minimal


@dataclass
class ScoredProspect:
    # Contact fields
    contact_name: str
    organization: str
    org_type: str
    role: str
    email: str
    region: str
    contact_status: str

    # Enrichment snapshot
    enrichment_summary: str
    aum_raw: str
    is_lp: bool
    sustainability_mandate: bool
    private_credit_allocation: bool
    emerging_manager_program: bool
    brand_recognition: str
    notable_facts: str

    # Dimension scores
    sector_fit_score: float
    sector_fit_reasoning: str
    sector_fit_confidence: str
    relationship_depth_score: float
    halo_value_score: float
    halo_value_reasoning: str
    halo_value_confidence: str
    emerging_fit_score: float
    emerging_fit_reasoning: str
    emerging_fit_confidence: str
    composite_score: float
    tier: str

    check_size_low: Optional[float]
    check_size_high: Optional[float]

    # Meta
    tokens_input: int
    tokens_output: int
    cost_usd: float
    enriched_at: str
    data_quality: str