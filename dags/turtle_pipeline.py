from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'yvonne',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='turtle_pipeline',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
    description='Weekly sea turtle occurrence pipeline',
) as dag:

    check_api_health = PythonOperator(
        task_id='check_api_health',
        python_callable=lambda: print("OBIS API health check — stub"),
    )

    fetch_obis_data = PythonOperator(
        task_id='fetch_obis_data',
        python_callable=lambda: print("Fetching OBIS data — stub"),
    )

    upload_to_gcs = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=lambda: print("Uploading to GCS bronze — stub"),
    )

    run_databricks_transform = PythonOperator(
        task_id='run_databricks_transform',
        python_callable=lambda: print("Running Databricks transform — stub"),
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

    check_api_health >> fetch_obis_data >> upload_to_gcs >> run_databricks_transform >> run_dbt_models >> run_dbt_tests >> score_anomalies >> notify_on_anomalies