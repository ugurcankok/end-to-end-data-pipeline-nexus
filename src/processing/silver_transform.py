import sys
import os
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import from_avro

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("SilverTransform")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
try:
    from utils.helpers import (
        BRONZE_PATH, 
        SILVER_PATH, 
        CHECKPOINT_SILVER
    )
except ImportError as e:
    logger.error(f"Helpers import hatası: {e}")
    raise

def run_silver_transformation():
    logger.info("Spark session is starting...")

    spark = SparkSession.builder \
        .appName("NexusLake-Silver-Transformation") \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    current_script_path = Path(__file__).resolve()
    project_root = current_script_path.parent.parent.parent
    schema_path = project_root / "src" / "schemas" / "transaction_schema.avsc"
    
    logger.info(f"Loading the Avro chart: {schema_path}")

    try:
        with open(schema_path, "r", encoding="utf-8") as f:
            avro_schema_json = f.read()
    except FileNotFoundError:
        logger.error(f"ERROR: Schema file not found: {schema_path}")
        return

    try:
        logger.info(f"Reading the Bronze data: {BRONZE_PATH}")
        bronze_df = spark.readStream.format("delta").load(BRONZE_PATH)
        
        # Magic byte removal and Avro parsing
        binary_df = bronze_df.withColumn("fixed_value", expr("substring(value, 6, length(value)-5)"))
        
        parsed_df = binary_df.select(
            from_avro(col("fixed_value"), avro_schema_json, {"mode": "PERMISSIVE"}).alias("parsed_data")
        ).select("parsed_data.*")
        
        silver_df = parsed_df.withColumn(
            "event_timestamp", 
            (col("timestamp") / 1000).cast("timestamp")
        )

        logger.info(f"Writing to the Silver layer begins (Delta). Target: {SILVER_PATH}")
        query = silver_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", CHECKPOINT_SILVER) \
            .trigger(availableNow=True) \
            .start(SILVER_PATH)

        query.awaitTermination()
        logger.info("The Silver Transformation has been successfully completed.")

    except Exception as e:
        logger.error(f"Error during the Silver transformation: {str(e)}")
        raise

if __name__ == "__main__":
    run_silver_transformation()