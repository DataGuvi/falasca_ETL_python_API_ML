"""One-off bootstrap entrypoint: run this once to backfill bronze/prata from
the seller's entire catalog (see src/extraction/full_scan.py). Do not add
this to the scheduled job -- only main.py (incremental, webhook-driven)
belongs there."""
from src.pipeline import run_full_scan

if __name__ == '__main__':
    run_full_scan()
