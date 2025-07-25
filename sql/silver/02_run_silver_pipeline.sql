-- 04_call_procedures.sql (revised to allow continuation)

-- Step 1: Always validate
CALL monitoring.validate_bronze_layer();

-- Step 2: Proceed to insert; each proc should filter invalid rows internally
CALL silver.insert_erp_px_cat();
CALL silver.insert_erp_loc_info();
CALL silver.insert_erp_cust_info();
CALL silver.insert_crm_cust_info();
CALL silver.insert_crm_prd_info();
CALL silver.insert_crm_sales_details();

-- Step 3: Validate after insert
CALL monitoring.validate_silver_layer();