import requests
import json
import os
from datetime import datetime, UTC
from google.cloud import storage

# GCS config
BUCKET_NAME = "turtletide-bronze-manifest-stream-452700-g7"
GCS_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

# OBIS config
SPECIES = {
    "dermochelys_coriacea": 137209,  # Leatherback
}
PAGE_SIZE = 10000
MAX_RECORDS = 50000 #temporary - remove it after we've verified that the pipeline works 

def fetch_obis_species(taxon_id: int, species_key: str):
    """Fetch all occurrence records for a species from OBIS, paginated."""
    all_records = []
    offset = 0

    print(f"Starting ingestion for {species_key} (taxonID={taxon_id})")

    while True:
        url = "https://api.obis.org/v3/occurrence"
        params = {
            "taxonid": taxon_id,
            "size": PAGE_SIZE,
            "offset": offset,
        }

        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        records = data.get("results", [])
        if not records:
            break

        all_records.extend(records)
        print(f"  Fetched {len(all_records)} records so far...")
        offset += PAGE_SIZE

        if len(all_records) >= MAX_RECORDS:
            print(f"  Reached max_records limit ({MAX_RECORDS}), stopping early.")
            break

        if len(records) < PAGE_SIZE:
            break

    print(f"Total records fetched for {species_key}: {len(all_records)}")
    return all_records


def upload_to_gcs(records: list, species_key: str, chunk_size: int = 1000):
    """Upload raw JSON records to GCS bronze bucket in chunks."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    date_str = datetime.now(UTC).strftime("%Y/%m/%d")
    chunks = [records[i:i + chunk_size] for i in range(0, len(records), chunk_size)]

    for idx, chunk in enumerate(chunks):
        blob_path = f"{species_key}/{date_str}/raw_page_{idx+1:03d}.json"
        blob = bucket.blob(blob_path)
        blob.upload_from_string(
            json.dumps(chunk, indent=2),
            content_type="application/json"
        )
        print(f"  Uploaded chunk {idx+1}/{len(chunks)} ({len(chunk)} records) to gs://{BUCKET_NAME}/{blob_path}")

    print(f"Upload complete — {len(records)} records in {len(chunks)} chunks")

def run():
    for species_key, taxon_id in SPECIES.items():
        records = fetch_obis_species(taxon_id, species_key)
        upload_to_gcs(records, species_key)


if __name__ == "__main__":
    run()