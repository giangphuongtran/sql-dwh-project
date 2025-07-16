-- 04_call_procedures.sql (revised to allow continuation)

-- Step 1: Always validate
CALL monitoring.validate_bronze_layer();

-- Step 2: Log summary of today's issues
INSERT INTO monitoring.data_validation_logs (check_name, issue_details)
SELECT 'summary', 'Total issues today: ' || COUNT(*) 
FROM monitoring.data_validation_logs
WHERE failed_at::date = CURRENT_DATE;

-- Step 3: Proceed to insert; each proc should filter invalid rows internally
CALL silver.insert_erp_px_cat();
CALL silver.insert_erp_loc_info();
CALL silver.insert_erp_cust_info();
CALL silver.insert_crm_cust_info();
CALL silver.insert_crm_prd_info();
CALL silver.insert_crm_sales_details();
