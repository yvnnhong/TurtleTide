# TurtleTide
### Global Sea Turtle Occurrence Analytics Pipeline

`Airflow` · `PySpark` · `Databricks` · `dbt` · `Delta Lake` · `GCS` · `BigQuery` · `Terraform` · `MLflow` · `Streamlit`

---

A weekly-automated data engineering pipeline that ingests global sea turtle occurrence records from the OBIS (Ocean Biodiversity Information System) REST API, transforms and models the data through a Bronze → Silver → Gold medallion architecture on Google Cloud Storage, detects anomalous sighting patterns using STL (Seasonal-Trend decomposition using LOESS), and surfaces findings through a live Streamlit dashboard with geospatial maps.

**Live Demo:** [turtletide.streamlit.app](https://turtletide.streamlit.app) · **Status:** Active

---

## What It Does

- Fetches OBIS occurrence records weekly via Airflow DAG across multiple sea turtle species — Green Turtle (*Chelonia mydas*, 213K+ records), Leatherback (*Dermochelys coriacea*), Loggerhead (*Caretta caretta*), and Hawksbill (*Eretmochelys imbricata*) — using chunked paginated ingestion (10K records per API call)
- Stores raw JSON in GCS bronze bucket and processes it through Bronze → Silver → Gold layers
- Cleans and types raw occurrence data with PySpark on Databricks — deduplicates records across overlapping OBIS datasets, standardizes species codes, assigns ocean basin per record using coordinate bounding boxes, handles missing `sst`/`sss`/`lifeStage` fields gracefully, writes Silver layer as a Delta Lake table on GCS for atomic commits and schema enforcement
- Models Silver-to-Gold transformations with dbt — staging, fact, and reporting mart layers with built-in data quality tests; dbt targets BigQuery as the Gold warehouse
- Detects anomalous sighting patterns per species per ocean basin using STL seasonal decomposition — decomposes weekly sighting counts into trend, seasonal, and residual components; flags basins where residual exceeds 2 standard deviations; correlates anomalies with sea surface temperature (`sst`) and salinity (`sss`) deviations where available; all experiment runs logged with MLflow
- Visualizes global turtle sighting events and regional anomalies on a live Streamlit dashboard with pydeck geospatial maps

---

## Tech Stack

| Layer | Tool |
|---|---|
| Infrastructure | Terraform (GCS buckets, BigQuery datasets, IAM) |
| Orchestration | Apache Airflow 2.x |
| Compute | Databricks (PySpark) |
| Storage | GCS (Bronze/Silver/Gold buckets) |
| Table Format | Delta Lake (Silver layer) |
| Data Modeling | dbt Core + dbt-bigquery |
| Warehouse | BigQuery (Gold layer) |
| ML Tracking | MLflow |
| Cloud | GCP |
| Dashboard | Streamlit + pydeck + Plotly |
| Language | Python 3.11 |
| Data Source | OBIS REST API (CC0 licensed) |
| Dev | Docker Compose, Git, GitHub Actions |

---

## Current Build Status

| Layer | Status | Details |
|---|---|---|
| Terraform | Live | GCS buckets + BigQuery datasets + IAM provisioned |
| Airflow DAG | Live | All tasks running end-to-end |
| Bronze ingest | Working | 213K+ Green Turtle records in GCS |
| Silver transform | Working | Cleaned records as Delta Lake table in GCS |
| dbt models | Working | 3 models, 6+ tests, all passing |
| STL + MLflow | Working | Scoring per species per ocean basin, experiment tracked |
| Streamlit dashboard | Live | turtletide.streamlit.app |

---

## Project Structure

```
turtletide/
├── terraform/
│   ├── main.tf                    # GCS buckets + BigQuery datasets + IAM
│   ├── variables.tf
│   └── outputs.tf
├── dags/
│   └── turtle_pipeline.py         # Main Airflow DAG (@weekly)
├── spark/
│   ├── bronze_ingest.py           # OBIS API → paginated JSON → GCS bronze
│   └── silver_transform.py        # GCS bronze → Delta Lake table → GCS silver
├── dbt_project/
│   └── turtletide/
│       └── models/
│           ├── staging/
│           │   ├── stg_obis_occurrences.sql
│           │   └── schema.yml
│           └── marts/
│               ├── fct_turtle_sightings.sql
│               └── rpt_basin_anomalies.sql
├── ml/
│   └── stl_anomaly_scorer.py      # STL decomposition + MLflow logging
├── dashboard/
│   ├── app.py                     # Streamlit dashboard
│   └── requirements.txt
├── docker-compose.yaml            # Local Airflow stack
├── .env.example                   # Environment variable template
└── README.md
```

---

## Getting Started

### Prerequisites

- Python 3.11+
- Docker Desktop (for local Airflow)
- GCP account with GCS and BigQuery access
- Databricks free tier account
- `pip install requests google-cloud-storage google-cloud-bigquery pandas pyarrow deltalake dbt-core dbt-bigquery mlflow statsmodels streamlit pydeck plotly`

### 1. Clone and configure

```bash
git clone https://github.com/yvnnhong/turtletide
cd turtletide
cp .env.example .env
# Fill in: GCP_PROJECT_ID, GCP_REGION, GOOGLE_APPLICATION_CREDENTIALS,
#          DATABRICKS_HOST, DATABRICKS_TOKEN
```

### 2. Provision infrastructure with Terraform

```bash
cd terraform
terraform init
terraform plan
terraform apply
# Provisions: GCS bronze/silver/gold buckets + BigQuery turtletide dataset + IAM service account
```

### 3. Load environment variables (PowerShell)

```powershell
foreach ($line in Get-Content .env) {
    if ($line -match '^([^=]+)=(.*)$') {
        [System.Environment]::SetEnvironmentVariable($matches[1], $matches[2])
    }
}
```

### 4. Start Airflow locally

```bash
docker-compose up -d
# Airflow UI → localhost:8080
# Username: airflow  Password: airflow
# Find turtle_pipeline DAG and trigger a run
```

### 5. Run the pipeline manually

```bash
python spark/bronze_ingest.py
python spark/silver_transform.py
```

### 6. Run dbt models

```bash
cd dbt_project/turtletide
dbt build --select stg_obis_occurrences fct_turtle_sightings rpt_basin_anomalies
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

The main DAG (`turtle_pipeline`) runs on a `@weekly` schedule. Tasks execute in the following order:

| Task | Operator | Description |
|---|---|---|
| `check_api_health` | PythonOperator | Confirms OBIS API is reachable before fetching |
| `fetch_obis_data` | PythonOperator | Pulls paginated occurrence records per species (10K records/call) |
| `upload_to_gcs` | PythonOperator | Confirms raw JSON is written to GCS bronze bucket |
| `run_databricks_transform` | PythonOperator | Triggers Databricks PySpark Silver-layer cleaning and Delta write |
| `run_dbt_models` | BashOperator | Runs dbt models (staging → marts) targeting BigQuery |
| `run_dbt_tests` | BashOperator | Runs dbt build with tests; fails DAG on data quality issues |
| `score_anomalies` | PythonOperator | STL decomposition per species per ocean basin; results logged to MLflow |
| `notify_on_anomalies` | PythonOperator | Prints alert if anomaly rate exceeds 5% threshold |

---

## Terraform Infrastructure

All GCP infrastructure is provisioned as code using Terraform — no manual console clicks.

```hcl
# terraform/main.tf
resource "google_storage_bucket" "bronze" {
  name     = "turtletide-bronze"
  location = var.region
}

resource "google_bigquery_dataset" "gold" {
  dataset_id = "turtletide_gold"
  location   = var.region
}
```

**Provisioned resources:**
- GCS buckets: `turtletide-bronze`, `turtletide-silver`, `turtletide-gold`
- BigQuery dataset: `turtletide_gold`
- IAM service account with Storage Object Admin + BigQuery Data Editor roles

---

## Delta Lake

The Silver layer writes data as a Delta Lake table on GCS instead of individual Parquet files. This means:

- **Atomic commits** — if a weekly pipeline run crashes mid-write, the partial write is rolled back automatically. BigQuery and dbt always read the last successfully committed version, keeping the live dashboard consistent.
- **Transaction log** — every write is recorded in `_delta_log/` alongside the Parquet data files.
- **Schema enforcement** — inconsistent fields across OBIS datasets (some records have `sst`/`sss`/`lifeStage`, others don't) are handled at write time rather than silently corrupting downstream models.

```python
# silver_transform.py — write
write_deltalake(gcs_path, arrow_table, mode="overwrite", storage_options=storage_options)
```

---

## GCS Bucket Structure

```
turtletide-bronze/
└── chelonia_mydas/2026/03/24/raw_page_001.json
└── chelonia_mydas/2026/03/24/raw_page_002.json
...

turtletide-silver/
└── chelonia_mydas/
    ├── _delta_log/               # Delta Lake transaction log
    │   └── 00000000000000000000.json
    └── part-00000-*.parquet      # Data files managed by Delta

turtletide-gold/
└── (dbt mart outputs → BigQuery)
```

---

## dbt Models

| Model | Layer | Description |
|---|---|---|
| `stg_obis_occurrences` | Staging | Reads GCS Silver Delta table, renames columns, filters null coordinates |
| `fct_turtle_sightings` | Mart | One row per occurrence event; adds ocean basin assignment |
| `rpt_basin_anomalies` | Mart | Weekly sighting counts per species per ocean basin for STL input |

### Tests

- `not_null` on `occurrence_id`, `event_date`, `latitude`, `longitude`, `species`
- `accepted_values` on `species` — only whitelisted study species pass through
- `accepted_range` on `latitude` (-90 to 90) and `longitude` (-180 to 180)

---

## STL Anomaly Detection

Each pipeline run applies STL (Seasonal-Trend decomposition using LOESS) to weekly sighting counts per species per ocean basin.

**How it works:**
1. Query BigQuery Gold mart for weekly sighting counts per species per basin
2. Decompose each time series into **trend** (long-term direction) + **seasonal** (expected annual nesting/migration cycle) + **residual** (unexplained variation)
3. Flag basins where the residual exceeds 2 standard deviations as anomalous — indicating sighting patterns beyond what season and trend predict
4. Where `sst` and `sss` data is available, correlate anomalies with environmental deviations to identify likely causes (cold stunning, harmful algal blooms, fishing pressure)

**Why STL over point-based methods:** Sea turtle sightings have strong seasonal patterns tied to nesting cycles and migration routes. Methods like Z-score or Isolation Forest would flag normal seasonal peaks as anomalies. STL explicitly separates the seasonal component first, detecting only genuinely unexpected patterns in the residual.

```python
from statsmodels.tsa.seasonal import STL

stl = STL(weekly_counts, period=52)  # 52 weeks = 1 year seasonal cycle
result = stl.fit()
residual = result.resid
anomalies = residual[abs(residual) > residual.std() * 2]
```

---

## MLflow Experiment Tracking

Every pipeline run logs a new experiment run under `turtletide`.

```bash
mlflow ui  # → localhost:5000
```

**Logged per run:**

| Type | Details |
|---|---|
| Parameters | `stl_period`, `anomaly_threshold_std`, `species_list` |
| Metrics | `mean_residual`, `std_residual`, `anomaly_rate`, `total_basins`, `anomaly_count` |
| Artifacts | STL decomposition plots per species per basin |

---

## Data Source

**OBIS (Ocean Biodiversity Information System)**
Hosted by the Intergovernmental Oceanographic Commission of UNESCO.
All occurrence data is CC0 (public domain).

| Species | Common Name | OBIS taxonid | Records |
|---|---|---|---|
| *Chelonia mydas* | Green Turtle | 137206 | 213,130+ |
| *Dermochelys coriacea* | Leatherback | 137209 | 23,405+ |
| *Caretta caretta* | Loggerhead | 137205 | TBD |
| *Eretmochelys imbricata* | Hawksbill | 137772 | TBD |

---

## Creator

Yvonne Hong <3

## License

MIT License. Occurrence data: OBIS contributors via Ocean Biodiversity Information System.