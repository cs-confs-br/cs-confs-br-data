import csv
import os
import sys
import requests
import json
import time

# ---- Config ----
MAX_TRIES = 40
CACHE_DIR = "cache"

# ---- Check args ----
if len(sys.argv) < 2:
    print("Usage: python script.py <API_KEY>")
    print("Get Meta API key at: https://datasolutions.springernature.com/account/api-management/")
    sys.exit(1)

api_key = sys.argv[1]

# ---- Ensure cache dir ----
os.makedirs(CACHE_DIR, exist_ok=True)

# ---- Load CSV ----
csv_file = "ABZ_2019_2024_springer_2025_09.confseries.csv"
with open(csv_file, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    dois = [row["DOI"].replace("https://doi.org/", "") for row in reader]

# ---- Iterate over DOIs ----
for base_doi in dois:
    print(f"\nProcessing base DOI: {base_doi}")

    for i in range(1, MAX_TRIES + 1):
        doi = f"{base_doi}_{i}"
        filename = doi.replace("/", "_").replace(":", "_") + ".json"
        filepath = os.path.join(CACHE_DIR, filename)
        print(f"  => target file is '{filepath}'")

        # Skip if already cached
        if os.path.exists(filepath):
            print(f"  Skipping cached {doi}")
            continue

        url = f"https://api.springernature.com/meta/v2/json?q=doi:{doi}&api_key={api_key}"
        print(f"  Fetching {doi} ...", end=" ")

        print("Be gentle... Wait a second!")
        time.sleep(1)  # pause for 1 second
        print("Go!")

        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"ERROR {e}")
            break

        # If no record, stop trying further suffixes
        if not data.get("records"):
            print("no record, stopping.")
            break

        # Save JSON
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        print("saved.")

print("\nAll done.")
