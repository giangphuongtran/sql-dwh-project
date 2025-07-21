-- Stored procedure to validate bronze layer and log issues
CREATE OR REPLACE PROCEDURE monitoring.validate_bronze_layer()
LANGUAGE plpgsql
AS $$
BEGIN
    -- erp_px_cat: duplicate ID
    INSERT INTO monitoring.data_validation_logs (table_name, record_key, column_name, error_reason, layer)
    SELECT 'erp_px_cat', "ID", 'id', 'Duplicate ID', 'bronze'
    FROM bronze.erp_px_cat
    GROUP BY "ID"
    HAVING COUNT(*) > 1;

    -- erp_px_cat: Nulls or dirty text
    INSERT INTO monitoring.data_validation_logs (table_name, record_key, column_name, error_reason, layer)
    SELECT 'erp_px_cat', "ID", col, err, 'bronze'
    FROM bronze.erp_px_cat,
         LATERAL (
            VALUES
                ('CAT', CASE WHEN "CAT" IS NULL THEN 'NULL' WHEN "CAT" <> TRIM("CAT") THEN 'Trim error' END),
                ('SUBCAT', CASE WHEN "SUBCAT" IS NULL THEN 'NULL' WHEN "SUBCAT" <> TRIM("SUBCAT") THEN 'Trim error' END),
                ('MAINTENANCE', CASE WHEN "MAINTENANCE" IS NULL THEN 'NULL' WHEN "MAINTENANCE" <> TRIM("MAINTENANCE") THEN 'Trim error' END)
         ) AS issues(col, err)
    WHERE err IS NOT NULL;

    -- crm_cust_info: duplicate or null ID
    INSERT INTO monitoring.data_validation_logs (table_name, record_key, column_name, error_reason, layer)
    SELECT 'crm_cust_info', CAST(cst_id AS TEXT), 'cst_id', 'Duplicate or NULL ID', 'bronze'
    FROM bronze.crm_cust_info
    GROUP BY cst_id
    HAVING COUNT(*) > 1 OR cst_id IS NULL;

    -- crm_cust_info: dirty first name
    INSERT INTO monitoring.data_validation_logs (table_name, record_key, column_name, error_reason, layer)
    SELECT 'crm_cust_info', CAST(cst_id AS TEXT), 'cst_firstname', 'Untrimmed first name', 'bronze'
    FROM bronze.crm_cust_info
    WHERE cst_firstname <> TRIM(cst_firstname);

    -- crm_cust_info: unexpected gender values (not F/M)
    INSERT INTO monitoring.data_validation_logs (table_name, record_key, column_name, error_reason, layer)
    SELECT 'crm_cust_info', CAST(cst_id AS TEXT), 'cst_gndr', 'Unexpected gender value', 'bronze'
    FROM bronze.crm_cust_info
    WHERE cst_gndr IS not NULL AND UPPER(TRIM(cst_gndr)) NOT IN ('F', 'M');

    -- crm_cust_info: invalid marital status
    INSERT INTO monitoring.data_validation_logs (table_name, record_key, column_name, error_reason, layer)
    SELECT 'crm_cust_info', CAST(cst_id AS TEXT), 'cst_marital_status', 'Unexpected status', 'bronze'
    FROM bronze.crm_cust_info
    WHERE cst_marital_status is not null AND UPPER(TRIM(cst_marital_status)) NOT IN ('S', 'M');

    -- crm_sales_details: sales logic issues
    INSERT INTO monitoring.data_validation_logs (table_name, record_key, column_name, error_reason, layer)
    SELECT 'crm_sales_details', sls_ord_num || '-' || sls_prd_key, 'sales_logic',
           'Mismatch or missing sales, quantity, or price', 'bronze'
    FROM bronze.crm_sales_details
    WHERE sls_quantity IS NULL OR sls_quantity <= 0
       OR sls_price IS NULL OR sls_price <= 0
       OR sls_sales IS NULL OR sls_sales <= 0
       OR sls_sales <> sls_quantity * ABS(sls_price);
END;
$$;
