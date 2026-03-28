variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "US"
}

variable "credentials_file" {
  description = "Path to GCP service account JSON key"
  type        = string
}