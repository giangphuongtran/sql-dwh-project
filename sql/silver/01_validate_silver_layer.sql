CREATE OR REPLACE PROCEDURE monitoring.validate_silver_layer()
LANGUAGE plpgsql
AS $$
BEGIN
    -- erp_px_cat: Nulls or dirty text
    INSERT INTO monitoring.data_validation_logs (layer, table_name, record_key, column_name, error_reason, detected_at)
    SELECT 'silver', 'erp_px_cat', id, col, err, current_timestamp
    FROM silver.erp_px_cat,
         LATERAL (
            VALUES
                ('cat', CASE WHEN cat IS NULL THEN 'NULL' WHEN cat <> TRIM(cat) THEN 'Trim error' END),
                ('subcat', CASE WHEN subcat IS NULL THEN 'NULL' WHEN subcat <> TRIM(subcat) THEN 'Trim error' END),
                ('maintenance', CASE WHEN maintenance IS NULL THEN 'NULL' WHEN maintenance <> TRIM(maintenance) THEN 'Trim error' END)
         ) AS issues(col, err)
    WHERE err IS NOT NULL;

    -- crm_cust_info: invalid gender (exclude NULL)
    INSERT INTO monitoring.data_validation_logs (layer, table_name, record_key, column_name, error_reason, detected_at)
    SELECT 'silver', 'crm_cust_info', cst_id, 'cst_gndr', 'Invalid gender value (e.g. N/A)', current_timestamp
    FROM silver.crm_cust_info
    WHERE cst_gndr IS NOT NULL AND cst_gndr NOT IN ('Male', 'Female');

    -- crm_cust_info: invalid marital status (exclude NULL)
    INSERT INTO monitoring.data_validation_logs (layer, table_name, record_key, column_name, error_reason, detected_at)
    SELECT 'silver', 'crm_cust_info', cst_id, 'cst_marital_status', 'Invalid marital status value (e.g. N/A)', current_timestamp
    FROM silver.crm_cust_info
    WHERE cst_marital_status IS NOT NULL AND cst_marital_status NOT IN ('Single', 'Married');

    -- crm_sales_details: sales logic errors
    INSERT INTO monitoring.data_validation_logs (layer, table_name, record_key, column_name, error_reason, detected_at)
    SELECT 'silver', 'crm_sales_details', sls_ord_num || '-' || sls_prd_key, 'sales_logic', 
           'Mismatch or missing sales, quantity, or price', current_timestamp
    FROM silver.crm_sales_details
    WHERE sls_quantity IS NULL OR sls_quantity <= 0
       OR sls_price IS NULL OR sls_price <= 0
       OR sls_sales IS NULL OR sls_sales <= 0
       OR sls_sales <> sls_quantity * ABS(sls_price);

    -- crm_sales_details: duplicate composite key
    INSERT INTO monitoring.data_validation_logs (layer, table_name, record_key, column_name, error_reason, detected_at)
    SELECT 'silver', 'crm_sales_details', sls_ord_num || '-' || sls_prd_key, 'composite_key', 
           'Duplicate composite key', current_timestamp
    FROM silver.crm_sales_details
    GROUP BY sls_ord_num, sls_prd_key
    HAVING COUNT(*) > 1;
END;
$$;
