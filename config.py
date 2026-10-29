from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
RAW = str(ROOT / "data" / "raw_events.csv")
CLEAN = str(ROOT / "data" / "clean_events_parquet")
OUT = ROOT / "output"
