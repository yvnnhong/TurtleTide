# TurtleTide
### Global Sea Turtle Occurrence Analytics Pipeline

`Airflow` · `PySpark` · `Dataproc` · `dbt` · `Delta Lake` · `GCS` · `BigQuery` · `Terraform` · `MLflow` · `Streamlit`

---

A weekly-automated data engineering pipeline that ingests global sea turtle occurrence records from the OBIS (Ocean Biodiversity Information System) REST API, transforms and models the data through a Bronze → Silver → Gold medallion architecture on Google Cloud Storage, detects anomalous sighting patterns using STL (Seasonal-Trend decomposition using LOESS), and surfaces findings through a live Streamlit dashboard with geospatial maps.

**Live Demo:** [turtletide.streamlit.app](https://turtletide.streamlit.app) · **Status:** Active

---

## What It Does

- Fetches OBIS occurrence records weekly via Airflow DAG across 4 sea turtle species — Green Turtle (*Chelonia mydas*), Leatherback (*Dermochelys coriacea*), Loggerhead (*Caretta caretta*), and Hawksbill (*Eretmochelys imbricata*) — using chunked paginated ingestion (10K records per API call)
- Stores raw JSON in GCS bronze bucket and processes it through Bronze → Silver → Gold layers
- Cleans and types raw occurrence data with PySpark on ephemeral Google Cloud Dataproc clusters — deduplicates records across overlapping OBIS datasets, standardizes field names, handles missing `sst`/`sss`/`lifeStage` fields gracefully, writes Silver layer as a Delta Lake table on GCS for atomic commits and schema enforcement. Dataproc clusters are created at job start and deleted immediately after to minimize cost.
- Models Silver-to-Gold transformations with dbt Core — staging, fact, and reporting mart layers with 7 built-in data quality tests; dbt targets BigQuery as the Gold warehouse
- Detects anomalous sighting patterns per species per ocean basin using STL seasonal decomposition — decomposes monthly sighting counts into trend, seasonal, and residual components; flags basins where residual exceeds 2 standard deviations; all experiment runs logged with MLflow
- Visualizes global turtle sighting events on a live Streamlit dashboard with pydeck geospatial maps and Plotly charts for SST, depth, life stage, and ocean basin distributions
- CI/CD via GitHub Actions — runs all 7 dbt data quality tests on every push to main

---

## Tech Stack

| Layer | Tool |
|---|---|
| Infrastructure | Terraform (GCS buckets, BigQuery datasets, IAM) |
| Orchestration | Apache Airflow 2.9 (Docker Compose) |
| Compute | Google Cloud Dataproc (ephemeral PySpark clusters) |
| Bronze Storage | GCS bronze bucket (raw JSON) |
| Silver Storage | GCS silver bucket (Delta Lake table) |
| Table Format | Delta Lake (Silver layer) |
| Data Modeling | dbt Core + dbt-bigquery |
| Gold Warehouse | BigQuery |
| ML Tracking | MLflow |
| Anomaly Detection | STL (Seasonal-Trend decomposition using LOESS) |
| Dashboard | Streamlit + pydeck + Plotly |
| Cloud | GCP (project: manifest-stream-452700-g7) |
| Language | Python 3.11 |
| Data Source | OBIS REST API (CC0 licensed) |
| Dev | Docker Compose, Git, GitHub Actions CI/CD |

---

## Current Build Status

| Layer | Status | Details |
|---|---|---|
| Terraform | Live | GCS buckets + BigQuery datasets + IAM provisioned |
| Airflow DAG | Live | All 7 tasks running end-to-end, zero stubs |
| Bronze ingest | Working | 4 species, 50K records each in GCS |
| Silver transform | Working | Cleaned records as Delta Lake tables in GCS per species |
| dbt models | Working | 3 models, 7 tests, all passing |
| STL + MLflow | Working | Scoring per species per ocean basin, experiment tracked |
| GitHub Actions | Live | dbt tests run on every push to main |
| Streamlit dashboard | Live | turtletide.streamlit.app |

---

## Project Structure

```
TurtleTide/
├── terraform/
│   ├── main.tf                    # GCS buckets + BigQuery datasets + IAM
│   ├── variables.tf
│   └── outputs.tf
├── dags/
│   └── turtle_pipeline.py         # Main Airflow DAG (@weekly, 7 tasks)
├── spark/
│   ├── bronze_ingest.py           # OBIS API → paginated JSON → GCS bronze
│   └── silver_transform.py        # GCS bronze → Delta Lake table → GCS silver (Dataproc)
├── turtletide/                    # dbt project
│   └── models/
│       ├── staging/
│       │   ├── stg_obis_occurrences.sql
│       │   └── sources.yml
│       └── marts/
│           ├── fct_turtle_sightings.sql
│           ├── rpt_basin_anomalies.sql
│           └── schema.yml
├── ml/
│   └── stl_anomaly_scorer.py      # STL decomposition + MLflow logging
├── dashboard/
│   ├── app.py                     # Streamlit dashboard
│   └── .streamlit/
│       └── config.toml            # Dark theme config
├── dbt_profiles/
│   └── profiles.yml               # dbt connection config for Docker
├── .github/
│   └── workflows/
│       └── dbt_ci.yml             # GitHub Actions CI/CD
├── load_silver_to_bq.py           # Loads Silver parquet files into BigQuery
├── docker-compose.yaml            # Local Airflow stack
└── README.md
```

---

## Getting Started

### Prerequisites

- Python 3.11 (use conda: `conda create -n turtletide python=3.11`)
- Docker Desktop (for local Airflow)
- GCP account with GCS, BigQuery, and Dataproc access
- Terraform installed and on PATH
- `gcloud` CLI installed (use cmd on Windows, not PowerShell)

### 1. Clone and configure

```bash
git clone https://github.com/yvnnhong/TurtleTide
cd TurtleTide
```

### 2. Set up GCP credentials

```powershell
# PowerShell — must re-run each new terminal session
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\Users\yvonn\TurtleTide\turtletide-key.json"
```

### 3. Provision infrastructure with Terraform

```bash
cd terraform
terraform init
terraform plan
terraform apply
# Provisions: GCS bronze/silver/gold buckets + BigQuery turtletide_gold + turtletide_silver datasets
```

### 4. Start Airflow locally

```bash
docker-compose up -d
# Airflow UI → localhost:8081
# Username: airflow  Password: airflow
```

### 5. Run the pipeline manually

```bash
# Ingest bronze data
python spark/bronze_ingest.py

# Run Dataproc silver transform (in cmd, not PowerShell)
gcloud dataproc clusters create turtletide-cluster --region=us-central1 --single-node --master-machine-type=n1-standard-2 --master-boot-disk-size=50GB --network=default --project=manifest-stream-452700-g7
gcloud dataproc jobs submit pyspark gs://turtletide-bronze-manifest-stream-452700-g7/scripts/silver_transform.py --cluster=turtletide-cluster --region=us-central1 --jars=gs://turtletide-bronze-manifest-stream-452700-g7/jars/delta-spark_2.12-3.2.0.jar,gs://turtletide-bronze-manifest-stream-452700-g7/jars/delta-storage-3.2.0.jar
gcloud dataproc clusters delete turtletide-cluster --region=us-central1 --quiet

# Load silver into BigQuery
python load_silver_to_bq.py
```

### 6. Run dbt models

```bash
cd turtletide
dbt run
dbt test
```

### 7. Run STL scorer

```bash
python ml/stl_anomaly_scorer.py
mlflow ui  # → localhost:5000
```

### 8. Launch the dashboard locally

```bash
cd dashboard
streamlit run app.py  # → localhost:8501
```

---

## Airflow DAG

The main DAG (`turtle_pipeline`) runs on a `@weekly` schedule. All 7 tasks are fully implemented — no stubs.

| Task | Operator | Description |
|---|---|---|
| `check_api_health` | PythonOperator | Confirms OBIS API is reachable before fetching |
| `fetch_obis_data` | PythonOperator | Pulls paginated occurrence records per species (10K records/call) |
| `run_dataproc_transform` | PythonOperator | Creates ephemeral Dataproc cluster, runs PySpark silver transform, deletes cluster |
| `run_dbt_models` | BashOperator | Runs `dbt run` targeting BigQuery gold dataset |
| `run_dbt_tests` | BashOperator | Runs `dbt test` — fails DAG on data quality issues |
| `score_anomalies` | PythonOperator | STL decomposition per species per ocean basin; results logged to MLflow |
| `notify_on_anomalies` | PythonOperator | Prints anomaly summary report to Airflow logs |

---

## Terraform Infrastructure

All GCP infrastructure is provisioned as code using Terraform — no manual console clicks.

**Provisioned resources:**
- GCS buckets: `turtletide-bronze`, `turtletide-silver`, `turtletide-gold`
- BigQuery datasets: `turtletide_gold`, `turtletide_silver`
- IAM service account (`turtletide-sa`) with Storage Admin, BigQuery Admin, Dataproc Admin roles

---

## Delta Lake

The Silver layer writes data as a Delta Lake table on GCS instead of individual Parquet files. This means:

- **Atomic commits** — if a weekly pipeline run crashes mid-write, the partial write is rolled back automatically
- **Transaction log** — every write is recorded in `_delta_log/` alongside the Parquet data files
- **Schema enforcement** — inconsistent fields across OBIS datasets are handled at write time rather than silently corrupting downstream models

---

## GCS Bucket Structure

```
turtletide-bronze/
└── dermochelys_coriacea/2026/03/29/raw_page_001.json ... raw_page_050.json
└── chelonia_mydas/2026/03/29/raw_page_001.json ... raw_page_050.json
└── caretta_caretta/2026/03/29/raw_page_001.json ... raw_page_050.json
└── eretmochelys_imbricata/2026/03/29/raw_page_001.json ... raw_page_002.json

turtletide-silver/
└── dermochelys_coriacea/
    ├── _delta_log/
    └── part-00000-*.parquet
└── chelonia_mydas/ ...
└── caretta_caretta/ ...
└── eretmochelys_imbricata/ ...
```

---

## dbt Models

| Model | Layer | Description |
|---|---|---|
| `stg_obis_occurrences` | Staging | Reads BigQuery silver table, parses dates, extracts year/month |
| `fct_turtle_sightings` | Mart | One row per occurrence event; adds ocean basin assignment from coordinates |
| `rpt_basin_anomalies` | Mart | Monthly sighting counts + avg SST/SSS/depth per species per ocean basin |

### Data Quality Tests (7 total)

- `not_null` on `occurrence_id`, `latitude`, `longitude`, `ocean_basin` (fct_turtle_sightings)
- `accepted_values` on `ocean_basin` — North Pacific, North Atlantic, South Atlantic, Indian Ocean, South Pacific, Other
- `not_null` on `ocean_basin`, `sighting_count` (rpt_basin_anomalies)

---

## STL Anomaly Detection

Each pipeline run applies STL (Seasonal-Trend decomposition using LOESS) to monthly sighting counts per species per ocean basin.

**How it works:**
1. Query BigQuery Gold mart for monthly sighting counts per species per basin
2. Decompose each time series into trend + seasonal + residual components
3. Flag basins where the residual exceeds 2 standard deviations as anomalous
4. Log all parameters, metrics, and results to MLflow

**Why STL:** Sea turtle sightings have strong seasonal patterns tied to nesting cycles and migration routes. STL explicitly separates the seasonal component first, detecting only genuinely unexpected patterns in the residual rather than flagging normal seasonal peaks.

---

## MLflow Experiment Tracking

Every pipeline run logs a new experiment run under `turtletide_stl_anomaly`.

**Logged per run:**

| Type | Details |
|---|---|
| Parameters | `model`, `period`, `threshold_std` |
| Metrics | `total_rows`, `total_anomalies`, `groups_scored` |
| Artifacts | `anomaly_results.csv` — per basin/species anomaly counts and residual stats |

---

## Data Source

**OBIS (Ocean Biodiversity Information System)**
Hosted by the Intergovernmental Oceanographic Commission of UNESCO.
All occurrence data is CC0 (public domain).

| Species | Common Name | OBIS taxonid |
|---|---|---|
| *Dermochelys coriacea* | Leatherback | 137209 |
| *Chelonia mydas* | Green Turtle | 137206 |
| *Caretta caretta* | Loggerhead | 137205 |
| *Eretmochelys imbricata* | Hawksbill | 137772 |

---

## Creator

Yvonne Hong

## License

MIT License. Occurrence data: OBIS contributors via Ocean Biodiversity Information System.