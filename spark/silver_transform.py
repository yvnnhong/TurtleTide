import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Config
GCS_BUCKET_BRONZE = "turtletide-bronze-manifest-stream-452700-g7"
GCS_BUCKET_SILVER = "turtletide-silver-manifest-stream-452700-g7"

SPECIES_LIST = [
    "dermochelys_coriacea",
    "chelonia_mydas",
    "caretta_caretta",
    "eretmochelys_imbricata",
]

DATE_PATH = datetime.now().strftime("%Y/%m/%d")


def build_spark():
    spark = SparkSession.builder \
        .appName("TurtleTide Silver Transform") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark


def transform_species(spark, species_key):
    bronze_path = f"gs://{GCS_BUCKET_BRONZE}/{species_key}/{DATE_PATH}/*.json"
    silver_path = f"gs://{GCS_BUCKET_SILVER}/{species_key}"

    print(f"Processing {species_key} from {bronze_path}...")

    try:
        df = spark.read.option("multiline", "true").json(bronze_path)
        print(f"  Raw record count: {df.count()}")

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

        print(f"  Clean record count: {df_clean.count()}")

        df_clean.write.format("delta").mode("overwrite").save(silver_path)
        print(f"  Written to {silver_path}")

    except Exception as e:
        print(f"  ERROR processing {species_key}: {e}")


def run():
    spark = build_spark()
    print("Spark session started!")

    for species_key in SPECIES_LIST:
        transform_species(spark, species_key)

    spark.stop()
    print("All species processed!")


if __name__ == "__main__":
    run()