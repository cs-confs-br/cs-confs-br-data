#!/usr/bin/env python3
import pandas as pd
import requests
import time
import csv
import os

INPUT_FILE = "ICVNS_2018_2024_springer_2025_09.confseries.csv"
OUTPUT_FILE = "ICVNS_2018_2024_OA_2025_09.OA-lite.csv"

# Fields we need
FIELDS = [
    "cited_by_count",
    "publication_year",
    "title",
    "display_name",
    "conference",
    "booktitle",
    "source",
    "primary_location.source.display_name",
    "primary_location.source.host_organization_name",
    "doi",
    "ids.doi",
    "authorships.author.display_name",
    "authorships.raw_author_name"
]

BASE_URL = "https://api.openalex.org/works/"

def fetch_openalex(doi: str):
    """Fetch metadata for a single DOI from OpenAlex"""
    url = f"{BASE_URL}https://doi.org/{doi}"
    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 404:
            return None  # signal to stop iteration
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Error fetching {doi}: {e}")
        return {}

def get_nested(data, path):
    """Safely get nested value from dict using dot notation"""
    value = data
    for part in path.split("."):
        if isinstance(value, dict) and part in value:
            value = value[part]
        else:
            return None
    return value

def main():
    df = pd.read_csv(INPUT_FILE)
    if "DOI" not in df.columns:
        raise ValueError("CSV must contain a 'DOI' column")
    dois = df["DOI"].dropna().unique()

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=FIELDS)
        writer.writeheader()

        for base_doi in dois:
            print(f"\n📘 Processing book DOI: {base_doi}")

            row = df[df["DOI"] == base_doi].iloc[0]

            # defaults
            first_paper = 1
            last_paper = 999

            if "FirstPaper" in df.columns and not pd.isna(row["FirstPaper"]):
                first_paper = int(row["FirstPaper"])
            if "LastPaper" in df.columns and not pd.isna(row["LastPaper"]):
                last_paper = int(row["LastPaper"])
                
            print(f"\n first paper = {first_paper}   last paper = {last_paper}")
            suffix = first_paper
            while True:
                chapter_doi = f"{base_doi}_{suffix}"
                print(f"   → Fetching chapter DOI: {chapter_doi}")
                data = fetch_openalex(chapter_doi)

                if data is None:
                    print(f"   × No more chapters (stopped at suffix {suffix})")
                    break
                elif not data:
                    print(f"   ! Skipped {chapter_doi}")
                else:
                    #row = {field: get_nested(data, field) for field in FIELDS}
                    row = {
                        "title": data.get("title"),
                        "display_name": data.get("display_name"),
                        "cited_by_count": data.get("cited_by_count", 0),
                        "publication_year": data.get("publication_year"),
                        "conference": data.get("biblio", {}).get("conference_name", ""),
                        "booktitle": data.get("biblio", {}).get("container_title", ""),
                        "source": data.get("primary_location", {}).get("source", {}).get("display_name", ""),
                        "primary_location.source.display_name": data.get("primary_location", {}).get("source", {}).get("host_organization_name", ""),
                        "primary_location.source.host_organization_name": data.get("primary_location", {}).get("source", {}).get("host_organization_name", ""),
                        "doi": data.get("doi") or data.get("ids", {}).get("doi", ""),
                        "ids.doi": data.get("ids", {}).get("doi", ""),
                        "authorships.author.display_name": " | ".join([
                            a.get("author", {}).get("display_name") or a.get("raw_author_name", "")
                            for a in data.get("authorships", [])
                        ]),
                        "authorships.raw_author_name": " | ".join([
                            a.get("raw_author_name", "")
                            for a in data.get("authorships", [])
                        ])
                    }

                    writer.writerow(row)

                    if True:
                        f_out.flush()     # ensure data is written immediately

                suffix += 1
                time.sleep(1)  # be nice to API
                if suffix > last_paper:
                    break

    print(f"\n✅ Done! Output saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
