from pipeline import run_pipeline
from db import init_db
from config import DB_PATH
import sys

if __name__ == "__main__":
    csv_file = sys.argv[1] if len(sys.argv) > 1 else "../../data/challenge_contacts.csv"
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else None
    run_id, costs = run_pipeline(csv_file, DB_PATH, limit=limit)
    print(f"Run {run_id} complete | Cost: ${costs['total_cost']:.2f}")