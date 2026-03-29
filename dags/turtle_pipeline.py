from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import requests
import subprocess

default_args = {
    'owner': 'yvonne',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def check_api_health_fn():
    """Ping OBIS API and confirm it's reachable."""
    url = "https://api.obis.org/v3/occurrence"
    params = {"taxonid": 137209, "size": 1}
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    print(f"OBIS API healthy — status {response.status_code}")


def fetch_and_upload_fn():
    """Fetch Leatherback records from OBIS and upload to GCS bronze bucket."""
    import sys
    sys.path.insert(0, '/opt/airflow/spark')
    from bronze_ingest import fetch_obis_species, upload_to_gcs, SPECIES

    for species_key, taxon_id in SPECIES.items():
        records = fetch_obis_species(taxon_id, species_key)
        upload_to_gcs(records, species_key)


def run_dataproc_transform_fn():
    from google.cloud import dataproc_v1
    from google.protobuf import duration_pb2
    import time

    project = "manifest-stream-452700-g7"
    region = "us-central1"
    cluster_name = "turtletide-cluster"
    script = "gs://turtletide-bronze-manifest-stream-452700-g7/scripts/silver_transform.py"
    jars = [
        "gs://turtletide-bronze-manifest-stream-452700-g7/jars/delta-spark_2.12-3.2.0.jar",
        "gs://turtletide-bronze-manifest-stream-452700-g7/jars/delta-storage-3.2.0.jar",
    ]

    cluster_client = dataproc_v1.ClusterControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )
    job_client = dataproc_v1.JobControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    # Step 1: Create cluster
    print("Creating Dataproc cluster...")
    cluster = {
        "project_id": project,
        "cluster_name": cluster_name,
        "config": {
            "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2"},
            "worker_config": {"num_instances": 0},
            "gce_cluster_config": {"network_uri": "default"},
            "software_config": {"image_version": "2.2"},
        },
    }
    operation = cluster_client.create_cluster(
        request={"project_id": project, "region": region, "cluster": cluster}
    )
    operation.result()
    print("Cluster created!")

    try:
        # Step 2: Submit PySpark job
        print("Submitting PySpark job...")
        job = {
            "placement": {"cluster_name": cluster_name},
            "pyspark_job": {
                "main_python_file_uri": script,
                "jar_file_uris": jars,
            },
        }
        result = job_client.submit_job_as_operation(
            request={"project_id": project, "region": region, "job": job}
        )
        response = result.result()
        print(f"Job finished with state: {response.status.state.name}")
        if response.status.state.name != "DONE":
            raise Exception(f"Job failed with state: {response.status.state.name}")

    finally:
        # Step 3: Always delete cluster
        print("Deleting cluster...")
        operation = cluster_client.delete_cluster(
            request={"project_id": project, "region": region, "cluster_name": cluster_name}
        )
        operation.result()
        print("Cluster deleted!")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id='turtle_pipeline',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
    description='Weekly sea turtle occurrence pipeline',
) as dag:

    check_api_health = PythonOperator(
        task_id='check_api_health',
        python_callable=check_api_health_fn,
    )

    fetch_obis_data = PythonOperator(
        task_id='fetch_obis_data',
        python_callable=fetch_and_upload_fn,
    )

    run_dataproc_transform = PythonOperator(
        task_id='run_dataproc_transform',
        python_callable=run_dataproc_transform_fn,
    )

    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command='echo "dbt models — stub"',
    )

    run_dbt_tests = BashOperator(
        task_id='run_dbt_tests',
        bash_command='echo "dbt tests — stub"',
    )

    score_anomalies = PythonOperator(
        task_id='score_anomalies',
        python_callable=lambda: print("STL anomaly scoring — stub"),
    )

    notify_on_anomalies = PythonOperator(
        task_id='notify_on_anomalies',
        python_callable=lambda: print("Anomaly notification — stub"),
    )

    check_api_health >> fetch_obis_data >> run_dataproc_transform >> run_dbt_models >> run_dbt_tests >> score_anomalies >> notify_on_anomalies