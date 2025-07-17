-- Create schema and log table to track data validation errors
CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS monitoring.data_validation_logs (
    log_id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    record_key TEXT NOT NULL,
    column_name TEXT NOT NULL,
    error_reason TEXT,
    detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.load_log (
    log_id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    load_status TEXT NOT NULL CHECK (load_status IN ('SUCCESS', 'FAIL')),
    error_message TEXT,
    log_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);