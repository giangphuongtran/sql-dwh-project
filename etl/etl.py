import os
import time
import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, input_file_name

# --- Set timezone to GMT+7 ---
os.environ['TZ'] = 'Asia/Ho_Chi_Minh'
time.tzset()

# --- Set up a function to ensure logs directory exist ---
def setup_logging(log_file_path, log_level=logging.INFO):
    '''
    Set up logging with both file and console handlers.
    Falls back to console-only logging if file creation fails.
    
    Args:
        log_file_path (str): Path to the log file.
        log_level: Logging level (default: INFO)
        
    Return:
        logging.Logger: Configured logger instance.
    '''
    log_dir = os.path.dirname(log_file_path)
    handlers = []
    
    if log_dir:
        try:
            os.makedirs(log_dir, exist_ok=True)
            handlers.append(logging.FileHandler(log_file_path))
        except OSError as e:
            print(f"Warning: Failed to created log directory '{log_dir}': {e}")
            print("Falling back to console-only logging.")
            
    # Always include console handler
    handlers.append(logging.StreamHandler())
    
    # Configure logging
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s | %(levelname)s | %(message)s',
        handlers=handlers,
        force=True
    )
    
    logger = logging.getLogger()
    
    if len(handlers) > 1:
        logger.info(f"Logger initialized with file logging to: {log_file_path}")
    else:
        logger.info("Logger initialized with console logging only")
        
    logging.getLogger("py4j").setLevel(logging.ERROR)
    
    return logger

# --- Setup Logging ---
log_file_path = "/logs/etl.log"
log = setup_logging(log_file_path)

# --- Check for ingestion toggle ---
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
    log.error(f"Missing environment variable: {e}. Please ensure PG_USER, PG_PASSWORD, PG_HOST, PG_PORT, PG_DB are set in Docker environment.")
    exit(1) # Exit if essential variables are missing
    
# --- Load Spark config ---
with open("config/spark_config.json") as f:
    spark_config = json.load(f)
    
jdbc_driver_path = spark_config["jdbc_driver_path"]  # required
spark_temp_dir = spark_config.get("temp_dir", "/tmp/spark-temp")

# --- Ensure Configure Spark Session with JDBC Driver ---
if not os.path.exists(jdbc_driver_path):
    log.error(f"JDBC driver not found at {jdbc_driver_path}. Please ensure it's mounted correctly in Docker container or available in the image.")
    exit(1)
else:
    log.info(f"JDBC driver found at {jdbc_driver_path}")
    
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

# --- Check if Spark temp dir is writable ---
if not os.access(spark_temp_dir, os.W_OK):
    log.error(f"Spark temporary directory {spark_temp_dir} is not writable by the current user. Check permissions inside the container.")
else:
    log.info(f"Spark temporary directory {spark_temp_dir} is writable")
    
# --- Configure Spark Session builder ---
spark_builder = SparkSession.builder \
    .appName("ETL CSV to Postgre Local") \
    .config("spark.jars", jdbc_driver_path) \
    .config("spark.local.dir", spark_temp_dir) \
    .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={spark_temp_dir}") \
    .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={spark_temp_dir}") \
    .config("spark.sql.warehouse", f"{spark_temp_dir}/spark-warehouse") \
    .config("spark.sql.session.local.dir", spark_temp_dir)
    # .config("spark.driver.maxResultSize", "4g") # Uncomment and adjust for large datasets
    # .config("spark.driver.memory", "4g") # Uncomment and adjust for memory issues
    # .config("spark.executor.memory", "4g") # Uncomment and adjust for memory issues
    
try:
    spark = spark_builder.getOrCreate()
    log.info("Spark Session created successfully.")
except Exception as e:
    log.error(f"Failed to create Spark Session: {e}", exc_info=True)
    exit(1)
    
log.info("--- End Set up configurations. ---")

# --- Starting ingestion ---
jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"

host_datasets_path = spark_config.get("host_datasets_path", "/data/datasets")

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
        log.warning(f"Directory not found in container: {full_path}. Skipping this doler. Check your Docker volume mounts.")
        continue
    
    try:
        files_in_dir = os.listdir(full_path)
        log.info(f"Contents found in {full_path}: {files_in_dir}")
        if not files_in_dir:
            log.info(f"No files found in {full_path}: {files_in_dir}")
            continue
    except OSError as e:
        log.error(f"Error listing directory {full_path}: {e}.Check directory existence and permissions inside the container.", exc_info=True)
        continue # Skip to next folder if directory cannot be listed
    
    for file in files_in_dir:
        log.info(f"Attempting to process file: {file}")
        
        if file.endswith('.csv') and file in file_table_map:
            table = file_table_map[file]
            file_path = os.path.join(full_path, file)
            log.info(f"     Recognized CSV file: {file_path}")
            log.info(f"     Targeting database table: bronze.{table}")
            
            try:
                log.info(f"     Attempting to read CSV file with Spark: {file_path}")
                df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
                df = df.withColumn("import_date", current_date()) \
                    .withColumn("source_file", input_file_name())
                log.info(f"     Successfully read {file}.")    
                
                log.info(f"     Writing DataFrame to bronze.{table} (mode: overwrite)")
                
                df.write \
                    .format('jdbc') \
                    .option('url', jdbc_url) \
                    .option('dbtable', f'bronze.{table}') \
                    .option('user', user) \
                    .option('password', password) \
                    .option('driver', 'org.postgresql.Driver') \
                    .mode('overwrite') \
                    .save()
                
                log.info(f"     Successfully ingested {file} to bronze.{table}")
            except Exception as e:
                log.error(f"    Error ingesting {file} to bronze.{table}: {e}", exc_info=True)
                log.error(f"    Ingestion failed for {file}. Continuing with next file if available.")
        else:
            log.info(f"     Skipping file '{file}' - not a recognized CSV or not in map")

log.info("All CSVs ingestion process completed (check database for actual data).")

# --- Stop Spark Session for clean shutdown ---
try:
    spark.stop()
    log.info("Spark Session stopped.")
except Exception as e:
    log.error(f"Error stopping Spark Session: {e}", exc_info=True)