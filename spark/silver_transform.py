import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit

# Config
GCS_BUCKET_BRONZE = "turtletide-bronze-manifest-stream-452700-g7"
GCS_BUCKET_SILVER = "turtletide-silver-manifest-stream-452700-g7"
SPECIES_KEY = "dermochelys_coriacea"
DATE_PATH = "2026/03/28"

BRONZE_PATH = f"gs://{GCS_BUCKET_BRONZE}/{SPECIES_KEY}/{DATE_PATH}/*.json"
SILVER_PATH = f"gs://{GCS_BUCKET_SILVER}/{SPECIES_KEY}"

def build_spark():
    spark = SparkSession.builder \
        .appName("TurtleTide Silver Transform") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .getOrCreate()
    return spark


def run():
    spark = build_spark()
    print("Spark session started!")

    print(f"Reading bronze JSON from {BRONZE_PATH}...")
    df = spark.read.option("multiline", "true").json(BRONZE_PATH)

    print(f"Raw record count: {df.count()}")
    print("Schema:")
    df.printSchema()

    # Select and clean relevant fields
    df_clean = df.select(
        col("id").alias("occurrence_id"),
        col("scientificName").alias("scientific_name"),
        col("species").alias("species"),
        col("decimalLatitude").alias("latitude"),
        col("decimalLongitude").alias("longitude"),
        col("eventDate").alias("event_date"),
        col("depth").alias("depth"),
        col("lifeStage").alias("life_stage"),
        col("basisOfRecord").alias("basis_of_record"),
        col("sst").alias("sea_surface_temp"),
        col("sss").alias("sea_surface_salinity"),
        col("dataset_id").alias("dataset_id"),
    ) \
    .filter(col("latitude").isNotNull()) \
    .filter(col("longitude").isNotNull()) \
    .filter(col("occurrence_id").isNotNull()) \
    .dropDuplicates(["occurrence_id"])

    print(f"Clean record count after filtering: {df_clean.count()}")

    # Write as Delta Lake table to GCS silver bucket
    print(f"Writing Silver Delta table to {SILVER_PATH}...")
    df_clean.write.format("delta").mode("overwrite").save(SILVER_PATH)
    print("Silver transform complete!")

    spark.stop()


if __name__ == "__main__":
    run()