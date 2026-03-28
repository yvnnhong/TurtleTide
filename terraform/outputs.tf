output "bronze_bucket_name" {
  description = "GCS bronze bucket name"
  value       = google_storage_bucket.bronze.name
}

output "silver_bucket_name" {
  description = "GCS silver bucket name"
  value       = google_storage_bucket.silver.name
}

output "gold_bucket_name" {
  description = "GCS gold bucket name"
  value       = google_storage_bucket.gold.name
}

output "bigquery_dataset_id" {
  description = "BigQuery gold dataset ID"
  value       = google_bigquery_dataset.gold.dataset_id
}