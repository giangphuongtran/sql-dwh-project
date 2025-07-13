-- 01_validate_bronze.sql
-- Stored procedure to validate bronze layer and log issues
CREATE OR REPLACE PROCEDURE monitoring.validate_bronze_layer()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Validate erp_px_cat: duplicate ID
    INSERT INTO monitoring.data_validation_logs (check_name, issue_details)
    SELECT 'duplicate_id_erp_px_cat', 'Duplicate ID: ' || "ID"
    FROM bronze.erp_px_cat
    GROUP BY "ID"
    HAVING COUNT(*) > 1;

    -- Nulls or dirty text in erp_px_cat
    INSERT INTO monitoring.data_validation_logs (check_name, issue_details)
    SELECT 'dirty_text_erp_px_cat', 'Trim/Clean failure at ID: ' || "ID"
    FROM bronze.erp_px_cat
    WHERE "CAT" IS NULL OR "SUBCAT" IS NULL OR "MAINTENANCE" IS NULL
       OR "CAT" <> TRIM("CAT") OR "SUBCAT" <> TRIM("SUBCAT") OR "MAINTENANCE" <> TRIM("MAINTENANCE");

    -- crm_cust_info: duplicate cst_id or nulls
    INSERT INTO monitoring.data_validation_logs (check_name, issue_details)
    SELECT 'duplicate_or_null_cust_id', 'Problem ID: ' || cst_id
    FROM bronze.crm_cust_info
    GROUP BY cst_id
    HAVING COUNT(*) > 1 OR cst_id IS NULL;

    -- crm_cust_info: dirty first name
    INSERT INTO monitoring.data_validation_logs (check_name, issue_details)
    SELECT 'dirty_firstname', 'First name dirty: ' || cst_firstname
    FROM bronze.crm_cust_info
    WHERE cst_firstname <> TRIM(cst_firstname);

    -- crm_cust_info: invalid gender
    INSERT INTO monitoring.data_validation_logs (check_name, issue_details)
    SELECT 'invalid_gender', 'Unexpected gender: ' || cst_gndr
    FROM bronze.crm_cust_info
    WHERE UPPER(TRIM(cst_gndr)) NOT IN ('F', 'M');

    -- crm_cust_info: invalid marital status
    INSERT INTO monitoring.data_validation_logs (check_name, issue_details)
    SELECT 'invalid_marital_status', 'Unexpected status: ' || cst_marital_status
    FROM bronze.crm_cust_info
    WHERE UPPER(TRIM(cst_marital_status)) NOT IN ('S', 'M');

    -- crm_sales_details: check price/quantity/sales logic
    INSERT INTO monitoring.data_validation_logs (check_name, issue_details)
    SELECT 'sales_logic_issue', 'sls_ord_num: ' || sls_ord_num || ', sls_sales: ' || sls_sales || ', quantity: ' || sls_quantity || ', price: ' || sls_price
    FROM bronze.crm_sales_details
    WHERE sls_quantity IS NULL OR sls_quantity <= 0
       OR sls_price IS NULL OR sls_price <= 0
       OR sls_sales IS NULL OR sls_sales <= 0
       OR sls_sales <> sls_quantity * ABS(sls_price);

END;
$$;