from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/yvonn/TurtleTide/turtletide-key.json"

client = bigquery.Client(project="manifest-stream-452700-g7")

SPECIES_LIST = [
    "dermochelys_coriacea",
    "chelonia_mydas",
    "caretta_caretta",
    "eretmochelys_imbricata",
]

for i, species_key in enumerate(SPECIES_LIST):
    gcs_uri = f"gs://turtletide-silver-manifest-stream-452700-g7/{species_key}/*.parquet"
    full_table_id = "manifest-stream-452700-g7.turtletide_silver.occurrences"

    print(f"Loading {species_key}...")

    load_job = client.load_table_from_uri(
        gcs_uri,
        full_table_id,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE if i == 0 else bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True,
        ),
    )
    load_job.result()
    print(f"  Done!")

table = client.get_table("manifest-stream-452700-g7.turtletide_silver.occurrences")
print(f"\nTotal rows in BigQuery silver: {table.num_rows}")