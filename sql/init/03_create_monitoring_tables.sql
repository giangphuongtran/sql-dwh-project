-- Create schema and log table to track data validation errors
CREATE SCHEMA IF NOT EXISTS monitoring;

--DROP TABLE IF exists monitoring.data_validation_logs;
CREATE TABLE IF NOT EXISTS monitoring.data_validation_logs (
    log_id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    record_key TEXT,
    column_name TEXT NOT NULL,
    error_reason TEXT,
    layer TEXT,
    detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);