"""Generate synthetic e-commerce clickstream data.
Usage: python src/generate_data.py --rows 500000
"""
import argparse, random, csv
from datetime import datetime, timedelta
from config import RAW

CATEGORIES = ["electronics", "fashion", "home", "beauty", "sports", "books"]
EVENTS = ["view", "cart", "purchase"]

def main(rows):
    random.seed(42)
    products = {f"P{i:04d}": (random.choice(CATEGORIES), round(random.uniform(5, 500), 2)) for i in range(1, 1001)}
    pids = list(products)
    start = datetime(2026, 9, 1)
    written = 0
    with open(RAW, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["event_time", "user_id", "session_hint", "event_type", "product_id", "category", "price"])
        while written < rows:
            u = f"U{random.randint(1, 20000):05d}"
            t = start + timedelta(seconds=random.randint(0, 30 * 24 * 3600))
            for _ in range(random.randint(2, 15)):  # one browsing session
                p = random.choice(pids); cat, price = products[p]
                ev = random.choices(EVENTS, weights=[80, 15, 5])[0]
                uid = "" if random.random() < 0.01 else u  # ~1% dirty rows
                w.writerow([t.isoformat(sep=" "), uid, "", ev, p, cat, price])
                written += 1
                t += timedelta(seconds=random.randint(5, 180))
    print(f"Wrote {written} rows to {RAW}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=500000)
    main(ap.parse_args().rows)
