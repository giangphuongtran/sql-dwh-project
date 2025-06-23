import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from sqlalchemy import create_engine, text
import logging

# Configure logging for better visibility
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    folder_path = params.folder_path

    # Construct JDBC URL outside SparkSession builder for clarity
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"
    db_properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }

    # Initialize SparkSession
    # No need to add .jars or .conf here; they are passed via spark-submit.
    spark = SparkSession.builder.appName("ETL CSV to Postgres") \
        .config("spark.jars", "/postgresql-42.6.0.jar") \
        .getOrCreate()

    logger.info("SparkSession initialized successfully.")

    # --- Database Schema Creation using SQLAlchemy ---
    # It's good to use SQLAlchemy for DDL (Data Definition Language) operations
    # like creating schemas/tables, as Spark's JDBC writer is primarily for DML (Data Manipulation Language).
    try:
        sqlalchemy_engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
        with sqlalchemy_engine.connect() as conn:
            # Use sqlalchemy.text for raw SQL to prevent SQL injection warnings/issues
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
            conn.commit() # Commit the changes
            logger.info("Schemas 'bronze' and 'silver' created (or already existed).")
    except Exception as e:
        logger.error(f"Error ensuring schemas exist: {e}")
        # Depending on severity, you might want to exit here if database setup is critical
        raise e

    file_table_map = {
        "cust_info.csv": "crm_cust_info",
        "prd_info.csv": "crm_prd_info",
        "sales_details.csv": "crm_sales_details",
        "CUST_AZ12.csv": "erp_cust_info",
        "LOC_A101.csv": "erp_loc_info",
        "PX_CAT_G1V2.csv": "erp_px_cat"
    }

    folders = ["source_crm", "source_erp"]

    logger.info("Starting CSV ingestion...")
    logger.debug(f"Base folder path: {folder_path}")

    for folder in folders:
        logger.info(f"Processing folder: {folder}")
        full_path_on_container = os.path.join(folder_path, folder)

        # Check if directory exists within the container's mounted path
        # Note: os.path.isdir checks the local file system where the script runs,
        # which is the container's file system in this case.
        if not os.path.isdir(full_path_on_container):
            logger.warning(f"Directory not found in container: {full_path_on_container}. Skipping.")
            continue # Skip to the next folder if it doesn't exist

        try:
            files_in_folder = os.listdir(full_path_on_container)
            logger.debug(f"Files found in {full_path_on_container}: {files_in_folder}")
        except OSError as e:
            logger.error(f"Could not list directory {full_path_on_container}: {e}. Skipping folder.")
            continue

        for file_name in files_in_folder:
            if file_name.endswith('.csv') and file_name in file_table_map:
                table_name = file_table_map[file_name]
                file_path_on_container = os.path.join(full_path_on_container, file_name)
                
                logger.info(f"  → Ingesting {file_name} to bronze.{table_name}")
                logger.debug(f"    Full file path for Spark: {file_path_on_container}")

                try:
                    # Spark can read directly from the mounted path inside the container
                    df = spark.read \
                        .option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv(file_path_on_container)

                    df.write \
                        .format('jdbc') \
                        .option('url', jdbc_url) \
                        .option('dbtable', f'bronze.{table_name}') \
                        .options(**db_properties) \
                        .mode('overwrite') \
                        .save()
                    
                    logger.info(f"Successfully ingested {file_name} to bronze.{table_name}")
                except AnalysisException as ae:
                    logger.error(f"Spark analysis error ingesting {file_name} (check CSV format or path): {ae}")
                except Exception as e:
                    logger.error(f"General error ingesting {file_name}: {e}")
            else:
                logger.debug(f"  Skipping non-CSV or untracked file: {file_name}")

    logger.info('All CSVs ingestion attempt completed.')
    spark.stop() # Good practice to stop SparkSession
    logger.info("SparkSession stopped.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="PySpark ETL script to ingest CSVs into PostgreSQL.")
    parser.add_argument("--user", required=True, help="PostgreSQL username")
    parser.add_argument("--password", required=True, help="PostgreSQL password")
    parser.add_argument("--host", required=True, help="PostgreSQL host (e.g., pgdatabase)")
    parser.add_argument("--port", type=int, required=True, help="PostgreSQL port (e.g., 5432)")
    parser.add_argument("--db", required=True, help="PostgreSQL database name")
    parser.add_argument("--folder_path", required=True, help="Base path to CSV folders (e.g., /data)")
    
    args = parser.parse_args()
    main(args)