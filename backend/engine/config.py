import os

GROQ_MODEL = "llama-3.3-70b-versatile"

os.makedirs("data", exist_ok=True)
os.makedirs("logs", exist_ok=True)

DB_PATH  = "data/pacezero.db"
LOG_PATH = "logs/enrichment.log"

DAILY_TOKEN_LIMIT = 100_000  # Groq free tier

# Groq free tier is $0.00 YAYYY
INPUT_TOKEN_COST_PER_M  = 0.0
OUTPUT_TOKEN_COST_PER_M = 0.0

# Composite score weights — must sum to 1.0
WEIGHTS = {
    "sector_fit":         0.35,
    "relationship_depth": 0.30,
    "halo_value":         0.20,
    "emerging_fit":       0.15,
}

# Allocation % used to estimate check size: AUM × pct → midpoint of range
ALLOCATION_PCT = {
    # Institutional LP types
    "Pension":              0.01,    # 0.5–2% of AUM
    "Insurance":            0.01,    # 0.5–2% of AUM
    "Endowment":            0.02,    # 1–3%
    "Foundation":           0.02,    # 1–3%
    "Fund of Funds":        0.035,   # 2–5%
    "Multi-Family Office":  0.035,   # 2–5%
    "Single Family Office": 0.065,   # 3–10%
    "HNWI":                 0.065,   # 3–10%
    "Asset Manager":        0.015,   # 0.5–3%
    "RIA/FIA":              0.015,   # 0.5–3% matches CSV org_type value
    "RIA":                  0.015,   # handles CSV rows with just "RIA"
    "FIA":                  0.015,   # handles CSV rows with just "FIA"
    "Private Capital Firm": 0.015,   # 0.5–3%
}