import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("GoldTransform")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
try:
    from utils.helpers import (
        SILVER_PATH, 
        GOLD_CUSTOMER_PATH, 
        GOLD_CURRENCY_PATH
    )
except ImportError as e:
    logger.error(f"Helpers import error: {e}")
    raise

def run_gold_transformation():
    logger.info("Spark session is starting (Gold Layer)...")

    spark = SparkSession.builder \
        .appName("NexusLake-Gold-Transformation") \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.properties.defaults.minReaderVersion", "1") \
        .config("spark.databricks.delta.properties.defaults.minWriterVersion", "2") \
        .config("spark.databricks.delta.properties.defaults.columnMapping.mode", "none") \
        .config("spark.databricks.delta.properties.defaults.enableDeletionVectors", "false") \
        .getOrCreate()

    logger.info("The Gold Transformation Process Has Begun")

    try:
        logger.info(f"Reading the Silver data from the source:: {SILVER_PATH}")
        silver_df = spark.read.format("delta").load(SILVER_PATH)

        #  Analysis 1: Total Spending by Customer
        logger.info("Müşteri harcama özeti hesaplanıyor...")
        customer_summary = silver_df.groupBy("customer_id") \
            .agg(
                sum("amount").alias("total_spent"),
                count("transaction_id").alias("transaction_count"),
                avg("amount").alias("avg_transaction_value")
            )

        # Analysis 2: Currency-Based Statistics
        logger.info("Currency statistics are being calculated...")
        currency_summary = silver_df.groupBy("currency") \
            .agg(
                sum("amount").alias("total_volume"),
                count("transaction_id").alias("total_count")
            )

        # Overwrite the Gold Layer
        logger.info(f"Customer summary is being generated: {GOLD_CUSTOMER_PATH}")
        customer_summary.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(GOLD_CUSTOMER_PATH)
        
        logger.info(f"Foreign exchange statistics are being compiled: {GOLD_CURRENCY_PATH}")
        currency_summary.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(GOLD_CURRENCY_PATH)

        logger.info("Gold Transformation Successfully Completed")

    except Exception as e:
        logger.error(f"Gold layer transformation error: {str(e)}")
        raise e
    finally:
        logger.info("The Spark session is closing...")
        spark.stop()

if __name__ == "__main__":
    run_gold_transformation()