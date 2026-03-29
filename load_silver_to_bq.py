from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/yvonn/TurtleTide/turtletide-key.json"

client = bigquery.Client(project="manifest-stream-452700-g7")

dataset_id = "turtletide_silver"
table_id = "occurrences"
full_table_id = f"manifest-stream-452700-g7.{dataset_id}.{table_id}"

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    autodetect=True,
)

gcs_uri = "gs://turtletide-silver-manifest-stream-452700-g7/dermochelys_coriacea/*.parquet"

print(f"Loading {gcs_uri} into {full_table_id}...")

load_job = client.load_table_from_uri(
    gcs_uri,
    full_table_id,
    job_config=job_config,
)

load_job.result()  # waits for job to complete

table = client.get_table(full_table_id)
print(f"✅ Done! Loaded {table.num_rows} rows into {full_table_id}")