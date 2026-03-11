# PaceZero LP Intelligence 🌿

> *An enrichment and scoring pipeline that helps fund managers find, understand, and prioritize their best LP prospects, powered by LLaMA 3.3 on Groq.*

---

## What This Is

PaceZero LP Intelligence is a full-stack pipeline that takes a CSV of fundraising contacts, enriches each organization using an LLM, scores every prospect across four dimensions, and surfaces the results in a clean, filterable web dashboard.

Built for PaceZero Capital Partners' Fund II raise (a sustainability-focused private credit fund based in Toronto), the system is designed to answer one question: **which LPs are most worth your time right now?**

It handles the boring parts (deduplication, caching, retries, fallbacks, cost tracking) so you can focus on the warm outreach.

---

## How It Works

```
CSV Upload -> Org Deduplication -> LLM Enrichment (Groq/LLaMA) -> Composite Scoring -> SQLite -> Flask API -> Dashboard
```

1. **You upload a contacts CSV** (or point the CLI at one)
2. **Contacts are deduplicated by organization.** One LLM call per org, no matter how many contacts come from the same institution.
3. **Each org is enriched.** The model returns structured JSON with mandate signals, AUM, LP/GP classification, and three dimension scores.
4. **Every contact is scored** using a weighted composite formula.
5. **Results land in SQLite** and are immediately queryable through the web UI.

---

## Scoring Formula

```
composite = sector_fit x 0.35 + relationship_depth x 0.30 + halo_value x 0.20 + emerging_fit x 0.15
```

| Dimension | Weight | What It Measures |
|---|---|---|
| **Sector Fit** | 35% | Does this LP allocate to private credit and have a sustainability mandate? |
| **Relationship Depth** | 30% | How warm is the existing relationship? (from your CRM data) |
| **Halo Value** | 20% | Would their commitment attract other LPs? |
| **Emerging Manager Fit** | 15% | Do they have a structural appetite for Fund I/II managers? |

Scores are on a 1-10 scale. Tiers:

- **Priority Close** -> composite >= 8.0
- **Strong Fit** -> composite >= 6.5
- **Moderate Fit** -> composite >= 5.0
- **Weak Fit** -> composite < 5.0

---

## Project Structure

```
pacezero/
├── backend/
│   ├── main.py               # CLI entry point
│   ├── pipeline.py           # Main orchestration loop
│   ├── enrichment_engine.py  # LLM prompts, parsing, fallback logic
│   ├── db.py                 # SQLite schema, caching, upserts
│   ├── model.py              # Dataclasses (Prospect, EnrichmentResult, ScoredProspect)
│   ├── config.py             # Weights, model name, token costs, allocation %
│   └── server.py             # Flask API + SPA serving
├── frontend/
│   └── index.html            # Single-file vanilla JS dashboard
├── data/
│   ├── challenge_contacts.csv
│   └── pacezero.db           # Created on first run
└── logs/
    └── enrichment.log
```

---

## Prerequisites

- **Python 3.10+**
- A free **Groq API key** from [console.groq.com](https://console.groq.com) (takes 30 seconds)
- That's genuinely it. No Node, no Docker, no external database.

---

## Setup

### 1. Clone and install dependencies

```bash
git clone https://github.com/your-org/pacezero-lp-intelligence.git
cd pacezero-lp-intelligence

pip install flask flask-cors groq
```

### 2. Set your Groq API key

```bash
export GROQ_API_KEY=gsk_your_key_here
```

Want this to persist across terminal sessions? Add it to your `~/.zshrc` or `~/.bashrc`:

```bash
echo 'export GROQ_API_KEY=gsk_your_key_here' >> ~/.zshrc
source ~/.zshrc
```

### 3. Make sure your CSV is in place

The default expected path is `data/challenge_contacts.csv`. The file should have these columns:

| Column | Required | Notes |
|---|---|---|
| `Contact Name` | yes | |
| `Organization` | yes | |
| `Org Type` | yes | e.g. Foundation, Pension, RIA |
| `Relationship Depth` | yes | Numeric 1-10 from your CRM |
| `Role` | optional | |
| `Email` | optional | |
| `Region` | optional | |
| `Contact Status` | optional | |

---

## Running the Pipeline (CLI)

```bash
# Run the full pipeline on your CSV
python backend/main.py

# Run on a specific file
python backend/main.py path/to/your_contacts.csv

# Limit to first 20 contacts (great for testing)
python backend/main.py data/challenge_contacts.csv 20

# Force re-enrich all orgs even if cached
python backend/main.py data/challenge_contacts.csv --refresh
```

You'll see live progress in the terminal:

```
[████████████░░░░░░░░] 12/94 orgs  (12%)
[run_20250301_143022] (13/94) Enriching: Rockefeller Foundation  [Jane Smith]
  ✓ sector=9.0  halo=9.0  emerging=8.0  quality=sufficient

[run_20250301_143022] (14/94) Enriching: Meridian Capital Group  [Bob Jones]
  ✓ sector=1.0  halo=3.0  emerging=1.0  quality=sufficient
```

---

## Running the Dashboard

```bash
python backend/server.py
```

Then open [http://localhost:5050](http://localhost:5050) in your browser.

The dashboard gives you:

- **Overview stats** with total prospects, tier distribution, and average scores
- **Filterable prospect table** where you can search by name, org, or role and sort by any dimension
- **Contact modal** with full enrichment detail, per-dimension reasoning, confidence levels, and estimated check size (click any row)
- **Score Breakdown** with dimension averages and top orgs by each score
- **Run History** with token usage and cost per pipeline run
- **Upload CSV** to drag-and-drop a new file and run the pipeline right from the browser

---

## Using the Web Upload

You don't have to use the CLI at all. Once the server is running:

1. Go to **Upload CSV** in the sidebar
2. Drag your file onto the drop zone (or click to browse)
3. Optionally check **Force refresh** to re-enrich cached orgs
4. Hit **Run Pipeline** and watch the progress bars fill in

The pipeline runs in a background thread and the UI polls for updates every 1.2 seconds.

---

## Configuration

Everything lives in `backend/config.py`:

```python
GROQ_MODEL = "llama-3.3-70b-versatile"  # swap providers here

WEIGHTS = {
    "sector_fit":         0.35,
    "relationship_depth": 0.30,
    "halo_value":         0.20,
    "emerging_fit":       0.15,
}

DAILY_TOKEN_LIMIT = 100_000  # Groq free tier limit
```

To change the scoring formula, just update `WEIGHTS` (they should sum to 1.0).

---

## A Note on the Free Tier

The Groq free tier gives you 100,000 tokens per day, which is enough to enrich roughly 90-130 unique organizations in a single run (depending on org name length and response verbosity).

If you hit the daily limit mid-run, the pipeline gracefully falls back to org-type defaults for the remaining orgs. You'll still get sensible scores rather than 5.0-across-the-board noise. Re-run the next day and it'll pick up where it left off, skipping already-cached orgs.

For larger lists (500+ unique orgs), you'll either want to run across multiple days or drop in a paid API key. The architecture makes provider-swapping a one-line change in `config.py`.

---

## Dependencies

```
flask          # web server
flask-cors     # CORS headers for local dev
groq           # official Groq Python SDK
```

Everything else (`sqlite3`, `csv`, `threading`, `json`, `dataclasses`) is Python standard library, so no extra installs needed beyond the three above.

---

## Troubleshooting

**"GROQ_API_KEY environment variable not set"**
Export the key in the same terminal session you're running the script from. See Setup step 2.

**"CSV not found"**
Make sure your file is at `data/challenge_contacts.csv` or pass the path explicitly:
```bash
python backend/main.py /full/path/to/file.csv
```

**Dashboard shows "No data yet"**
Run the pipeline first (CLI or web upload), then refresh the dashboard.

**Scores look wrong for a specific org**
The LLM works from training data, not live web search. For orgs with limited public information, scores may be lower confidence. The modal view shows reasoning and confidence per dimension, so look for `low` confidence flags. A `--refresh` re-run can help if you suspect a bad first pass.

**Port 5050 already in use**
```bash
PORT=5051 python backend/server.py
```

---

## What's Not Here (Yet)

A few things that would make this meaningfully better with more time:

- **Live web search** by connecting Tavily or Brave so enrichment pulls current press releases and LP announcements, not just training data
- **Async processing** with a token-bucket rate limiter that would cut a 1,000-org run from ~50 minutes to under 10
- **Fundraiser feedback loop** that lets the team flag incorrect scores, feeding them back as examples on the next run
- **Confidence-gated review queue** that routes low-confidence scores to a human review bucket rather than publishing them alongside high-confidence ones

See the **About & Design** page in the dashboard for a full writeup of design decisions and honest tradeoffs.

---

## License

MIT. Do what you'd like with it.

---

*Built with care for PaceZero Capital Partners. Questions? Open an issue or reach out directly.*
