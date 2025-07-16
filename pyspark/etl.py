import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, input_file_name
from sqlalchemy import create_engine
import sqlalchemy
import logging
import time
import shutil # Imported for potential future use or manual cleanup, not directly used here

# --- Set timezone to GMT+7 ---
os.environ['TZ'] = 'Asia/Ho_Chi_Minh'
time.tzset()

# Ensure the logs directory exists within the container
def setup_logging(log_file_path, log_level=logging.INFO):

    log_dir = os.path.dirname(log_file_path)
    handlers = []

    if log_dir:
        try:
            os.makedirs(log_dir, exist_ok=True)
            handlers.append(logging.FileHandler(log_file_path))
        except OSError as e:
            print(f"Warning: Failed to created log directory '{log_dir}': {e}")
            print("Falling back to console-only logging.")

    handlers.append(logging.StreamHandler())

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s | %(levelname)s | %(message)s',
        handlers=handlers,
        force=True
    )

    logger = logging.getLogger()

    # Log initialization
    if len(handlers) > 1:
        logger.info(f"Logger initialized with file logging to: {log_file_path}")
    else:
        logger.info("Logger initialized with console logging only")

    # Suppress noisy Spark gateway logs
    logging.getLogger("py4j").setLevel(logging.ERROR)
    
    return logger

# --- Setup Logging ---
log_file_path = "/logs/etl.log"
log = setup_logging(log_file_path)

run_ingest = os.getenv("RUN_INGEST", "false").lower() == "true"
if not run_ingest:
    log.info("RUN_INGEST is false. Skipping ingestion.")
    exit(0)
    
log.info("Starting ETL script.")

# --- Load environment variables ---
try:
    user = os.environ['PG_USER']
    password = os.environ['PG_PASSWORD']
    host = os.environ['PG_HOST']
    port = os.environ['PG_PORT']
    db = os.environ['PG_DB']
    log.info("Environment variables loaded successfully.")
except KeyError as e:
    log.error(f"Missing environment variable: {e}. Please ensure PG_USER, PG_PASSWORD, PG_HOST, PG_PORT, PG_DB are set in your Docker environment.")
    exit(1) # Exit if essential variables are missing

# --- Database Schema Creation ---
try:
    log.info(f"Attempting to connect to PostgreSQL at {host}:{port}/{db}")
    sqlalchemy_engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    with sqlalchemy_engine.connect() as conn:
        conn.execute(sqlalchemy.text("CREATE SCHEMA IF NOT EXISTS bronze"))
        conn.execute(sqlalchemy.text("CREATE SCHEMA IF NOT EXISTS silver"))
        conn.commit()
        log.info("Schemas 'bronze' and 'silver' created (or already existed).")
except Exception as e:
    log.error(f"Error connecting to database or creating schemas: {e}", exc_info=True)
    exit(1) # Exit if database setup fails

# --- IMPORTANT: Configure Spark Session with JDBC Driver ---
jdbc_driver_path = "/opt/jars/postgresql-42.6.0.jar"
if not os.path.exists(jdbc_driver_path):
    log.error(f"JDBC driver not found at {jdbc_driver_path}. Please ensure it's mounted correctly in your Docker container or available in the image.")
    exit(1)
else:
    log.info(f"JDBC driver found at {jdbc_driver_path}.")

# Define Spark temp directory within the container
spark_temp_dir = "/tmp/spark-temp"

# --- Pre-check and create Spark temporary directory ---
if not os.path.exists(spark_temp_dir):
    try:
        os.makedirs(spark_temp_dir)
        log.info(f"Created Spark temporary directory: {spark_temp_dir}")
    except OSError as e:
        log.error(f"Failed to create Spark temporary directory {spark_temp_dir}: {e}. This indicates a permissions or disk space issue within the container.")
        exit(1)
else:
    log.info(f"Spark temporary directory {spark_temp_dir} already exists.")

# Check if the directory is writable
if not os.access(spark_temp_dir, os.W_OK):
    log.error(f"Spark temporary directory {spark_temp_dir} is not writable by the current user. Check permissions inside the container.")
    exit(1)
else:
    log.info(f"Spark temporary directory {spark_temp_dir} is writable.")


# Configure Spark Session builder
spark_builder = SparkSession.builder \
    .appName("ETL CSV to Postgres Local") \
    .config("spark.jars", jdbc_driver_path) \
    .config("spark.local.dir", spark_temp_dir) \
    .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={spark_temp_dir}") \
    .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={spark_temp_dir}") \
    .config("spark.sql.warehouse.dir", f"{spark_temp_dir}/spark-warehouse") \
    .config("spark.sql.session.local.dir", spark_temp_dir) # THIS IS CRITICAL FOR SPARK SQL
    # .config("spark.driver.maxResultSize", "4g") # Uncomment and adjust for large datasets
    # .config("spark.driver.memory", "4g") # Uncomment and adjust for memory issues
    # .config("spark.executor.memory", "4g") # Uncomment and adjust for memory issues

try:
    spark = spark_builder.getOrCreate()
    log.info("Spark Session created successfully.")
except Exception as e:
    log.error(f"Failed to create Spark Session: {e}", exc_info=True)
    exit(1)

# --- Print Spark Configurations to verify spark.local.dir and other settings ---
log.info("Spark Configurations (as seen by SparkContext):")
for key, value in spark.sparkContext.getConf().getAll():
    log.info(f"  {key}: {value}")
log.info("--- End Spark Configurations ---")

jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"

# This is the path to our datasets folder ON DOCKER (mounted from host)
host_datasets_path = "/data/datasets"

file_table_map = {
    "cust_info.csv": "crm_cust_info",
    "prd_info.csv": "crm_prd_info",
    "sales_details.csv": "crm_sales_details",
    "CUST_AZ12.csv": "erp_cust_info",
    "LOC_A101.csv": "erp_loc_info",
    "PX_CAT_G1V2.csv": "erp_px_cat"
}

folders = ["source_crm", "source_erp"]

log.info('Starting CSV ingestion...')
log.info(f"Configured Host_datasets_path (inside container): {host_datasets_path}")
log.info(f"Folders to process: {folders}")


for folder in folders:
    log.info(f"\nProcessing folder: {folder}")
    full_path = os.path.join(host_datasets_path, folder)
    log.info(f"Full path for current folder: {full_path}")

    if not os.path.isdir(full_path):
        log.warning(f"Directory not found in container: {full_path}. Skipping this folder. Check your Docker volume mounts.")
        continue

    try:
        files_in_dir = os.listdir(full_path)
        log.info(f"Contents found in {full_path}: {files_in_dir}")
        if not files_in_dir:
            log.info(f"No files found in {full_path}. Skipping.")
            continue
    except OSError as e:
        log.error(f"Error listing directory {full_path}: {e}. Check directory existence and permissions inside the container.", exc_info=True)
        continue # Skip to next folder if directory cannot be listed

    for file in files_in_dir:
        log.info(f"Attempting to process file: {file}")

        if file.endswith('.csv') and file in file_table_map:
            table = file_table_map[file]
            file_path = os.path.join(full_path, file)
            log.info(f"    Recognized CSV file: {file_path}")
            log.info(f"    Targeting database table: bronze.{table}")

            try:
                # Add a log right before reading the CSV
                log.info(f"    Attempting to read CSV file with Spark: {file_path}")
                df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
                df = df.withColumn("import_date", current_date()) \
                    .withColumn("source_file", input_file_name()    )
                log.info(f"    Successfully read {file}.")
                # df.show(5, truncate=False) # Uncomment for quick data preview, but can be slow for very wide tables

                # To avoid expensive count() on large datasets, you might skip this or sample
                log.info(f"    Writing DataFrame to bronze.{table} (mode: overwrite)")
                df.write \
                    .format('jdbc') \
                    .option('url', jdbc_url) \
                    .option('dbtable', f'bronze.{table}') \
                    .option('user', user) \
                    .option('password', password) \
                    .option('driver', 'org.postgresql.Driver') \
                    .mode('overwrite') \
                    .save()
                log.info(f"    Successfully ingested {file} to bronze.{table}")
            except Exception as e:
                log.error(f"    Error ingesting {file} to bronze.{table}: {e}", exc_info=True)
                # This will print the full Java stack trace from Spark errors in your logs.
                log.error(f"    Ingestion failed for {file}. Continuing with next file if available.")
        else:
            log.info(f"    Skipping file '{file}' - not a recognized CSV or not in map.")

log.info('All CSVs ingestion process completed (check database for actual data).')

log.info("Starting to create empty tables in 'silver' schema...")

for file, table in file_table_map.items():
    bronze_table = f"bronze.{table}"
    silver_table = f"silver.{table}"

    try:
        log.info(f"Reading schema from {bronze_table}...")
        df_bronze = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", bronze_table) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .load()
            
        columns_to_remove = ["import_date", "source_file"]
        
        df_silver = df_bronze.drop(*columns_to_remove) \
            .withColumn("updated_dttm", current_date())

        log.info(f"Writing empty table {silver_table} with structure only...")
        df_silver.limit(0).write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", silver_table) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        
        log.info(f"Empty table created: {silver_table}")
    except Exception as e:
        log.error(f"Error creating table {silver_table}: {e}", exc_info=True)

# Stop Spark Session for clean shutdown
try:
    spark.stop()
    log.info("Spark Session stopped.")
except Exception as e:
    log.error(f"Error stopping Spark Session: {e}", exc_info=True)
