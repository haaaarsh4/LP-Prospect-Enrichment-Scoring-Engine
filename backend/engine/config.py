import os

MODEL = "gemini-2.0-flash"
DB_PATH = "data/pacezero.db"
LOG_PATH = "logs/enrichment.log"

# Gemini 2.0 Flash is FREE YAYYYY (1500 req/day)
INPUT_TOKEN_COST_PER_M  = 0.0
OUTPUT_TOKEN_COST_PER_M = 0.0

WEIGHTS = {
    "sector_fit": 0.35,
    "relationship_depth": 0.30,
    "halo_value": 0.20,
    "emerging_fit": 0.15,
}

ALLOCATION_PCT = {
    "Pension":              0.01,   # 0.5–2%
    "Insurance":            0.01,
    "Endowment":            0.02,   # 1–3%
    "Foundation":           0.02,
    "Fund of Funds":        0.035,  # 2–5%
    "Multi-Family Office":  0.035,
    "Single Family Office": 0.065,  # 3–10%
    "HNWI":                 0.065,
    "Asset Manager":        0.015,  # 0.5–3%
    "RIA/FIA":              0.015,
    "Private Capital Firm": 0.015,
}