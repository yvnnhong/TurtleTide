import os
import pandas as pd
import mlflow
import mlflow.sklearn
from google.cloud import bigquery
from statsmodels.tsa.seasonal import STL

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/yvonn/TurtleTide/turtletide-key.json"

PROJECT = "manifest-stream-452700-g7"
DATASET = "turtletide_gold"
TABLE = "rpt_basin_anomalies"

def load_data():
    client = bigquery.Client(project=PROJECT)
    query = f"""
        SELECT ocean_basin, scientific_name, year, month, sighting_count, avg_sst
        FROM `{PROJECT}.{DATASET}.{TABLE}`
        ORDER BY ocean_basin, scientific_name, year, month
    """
    return client.query(query).to_dataframe()


def score_anomalies(df):
    results = []

    groups = df.groupby(["ocean_basin", "scientific_name"])

    for (basin, species), group in groups:
        group = group.sort_values(["year", "month"]).reset_index(drop=True)

        # need at least 2 full seasons (24 months) for STL
        if len(group) < 24:
            print(f"Skipping {basin} / {species} — only {len(group)} months of data")
            continue

        series = group["sighting_count"].values

        stl = STL(series, period=12, robust=True)
        res = stl.fit()

        residuals = res.resid
        mean_resid = residuals.mean()
        std_resid = residuals.std()

        # flag anomalies as points more than 2 std devs from mean
        anomalies = abs(residuals - mean_resid) > 2 * std_resid
        anomaly_count = int(anomalies.sum())

        results.append({
            "ocean_basin": basin,
            "scientific_name": species,
            "n_months": len(group),
            "anomaly_count": anomaly_count,
            "mean_residual": round(float(mean_resid), 4),
            "std_residual": round(float(std_resid), 4),
        })

        print(f"{basin} / {species}: {anomaly_count} anomalies detected out of {len(group)} months")

    return results


def main():
    mlflow.set_experiment("turtletide_stl_anomaly")

    with mlflow.start_run():
        mlflow.log_param("model", "STL")
        mlflow.log_param("period", 12)
        mlflow.log_param("threshold_std", 2)

        print("Loading data from BigQuery...")
        df = load_data()
        print(f"Loaded {len(df)} rows")
        mlflow.log_metric("total_rows", len(df))

        print("Running STL anomaly scoring...")
        results = score_anomalies(df)

        total_anomalies = sum(r["anomaly_count"] for r in results)
        mlflow.log_metric("total_anomalies", total_anomalies)
        mlflow.log_metric("groups_scored", len(results))

        if results:
            results_df = pd.DataFrame(results)
            results_df.to_csv("ml/anomaly_results.csv", index=False)
            mlflow.log_artifact("ml/anomaly_results.csv")
            print(results_df)
        else:
            print("No groups had enough data for STL scoring")

        print(f"\nDone! {total_anomalies} total anomalies across {len(results)} groups")


if __name__ == "__main__":
    main()