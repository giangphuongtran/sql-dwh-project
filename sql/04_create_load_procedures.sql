-- Procedure to load erp_px_cat
CREATE OR REPLACE PROCEDURE silver.insert_erp_px_cat()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp();
    v_end_time TIMESTAMP;
    v_rows_inserted INTEGER;
BEGIN
    BEGIN
        -- Clear target table before load
        TRUNCATE TABLE silver.erp_px_cat;

        -- Insert only valid records
        INSERT INTO silver.erp_px_cat (id, cat, subcat, maintenance)
        SELECT
            REPLACE("ID", '_', '-') AS id,
            TRIM("CAT"),
            TRIM("SUBCAT"),
            TRIM("MAINTENANCE")
        FROM bronze.erp_px_cat
        WHERE REPLACE("ID", '_', '-') NOT IN (
            SELECT record_key
            FROM monitoring.data_validation_logs
            WHERE table_name = 'erp_px_cat'
        );

        GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;
        v_end_time := clock_timestamp();

        -- Success log
        INSERT INTO silver.load_log(table_name, load_status, load_time, load_start_time, load_end_time, rows_loaded)
        VALUES ('erp_px_cat', 'SUCCESS', v_end_time - v_start_time, v_start_time, v_end_time, v_rows_inserted);

    EXCEPTION WHEN OTHERS THEN
        v_end_time := clock_timestamp();
        INSERT INTO silver.load_log(table_name, load_status, load_time, load_start_time, load_end_time, rows_loaded, error_message)
        VALUES ('erp_px_cat', 'FAIL', v_end_time - v_start_time, v_start_time, v_end_time, 0, SQLERRM);
        RETURN;
    END;
END;
$$;

-- Procedure to load erp_loc_info
CREATE OR REPLACE PROCEDURE silver.insert_erp_loc_info()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp();
    v_end_time TIMESTAMP;
    v_rows_inserted INTEGER;
BEGIN
    BEGIN
        TRUNCATE TABLE silver.erp_loc_info;

        INSERT INTO silver.erp_loc_info (cid, cntry)
        SELECT
            REPLACE("CID", '-', '') AS cid,
            CASE
                WHEN "CNTRY" = 'DE' THEN 'Germany'
                WHEN "CNTRY" IN ('US', 'USA') THEN 'United States'
                WHEN "CNTRY" IS NULL THEN 'N/A'
                ELSE TRIM("CNTRY")
            END AS cntry
        FROM bronze.erp_loc_info;

        GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;
        v_end_time := clock_timestamp();

        INSERT INTO silver.load_log(table_name, load_status, load_time, load_start_time, load_end_time, rows_loaded)
        VALUES ('erp_loc_info', 'SUCCESS', v_end_time - v_start_time, v_start_time, v_end_time, v_rows_inserted);

    EXCEPTION WHEN OTHERS THEN
        v_end_time := clock_timestamp();
        INSERT INTO silver.load_log(table_name, load_status, load_time, load_start_time, load_end_time, rows_loaded, error_message)
        VALUES ('erp_loc_info', 'FAIL', v_end_time - v_start_time, v_start_time, v_end_time, 0, SQLERRM);
        RETURN;
    END;
END;
$$;

-- Procedure to load erp_cust_info
CREATE OR REPLACE PROCEDURE silver.insert_erp_cust_info()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp();
    v_end_time TIMESTAMP;
    v_rows_inserted INTEGER;
BEGIN
    BEGIN
        TRUNCATE TABLE silver.erp_cust_info;

        INSERT INTO silver.erp_cust_info (cid, bdate, gen)
        SELECT
            CASE
                WHEN "CID" LIKE 'NAS%' THEN SUBSTRING("CID" FROM 4)
                ELSE "CID"
            END AS cid,
            "BDATE",
            CASE
                WHEN UPPER(TRIM("GEN")) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM("GEN")) IN ('M', 'MALE') THEN 'Male'
                ELSE 'N/A'
            END AS gen
        FROM bronze.erp_cust_info
        WHERE "BDATE" < CURRENT_DATE;

        GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;
        v_end_time := clock_timestamp();

        INSERT INTO silver.load_log(table_name, load_status, load_time, load_start_time, load_end_time, rows_loaded)
        VALUES ('erp_cust_info', 'SUCCESS', v_end_time - v_start_time, v_start_time, v_end_time, v_rows_inserted);

    EXCEPTION WHEN OTHERS THEN
        v_end_time := clock_timestamp();
        INSERT INTO silver.load_log(table_name, load_status, load_time, load_start_time, load_end_time, rows_loaded, error_message)
        VALUES ('erp_cust_info', 'FAIL', v_end_time - v_start_time, v_start_time, v_end_time, 0, SQLERRM);
        RETURN;
    END;
END;
$$;

-- Procedure to load crm_cust_info
CREATE OR REPLACE PROCEDURE silver.insert_crm_cust_info()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp();
    v_end_time TIMESTAMP;
    v_rows_inserted INTEGER;
BEGIN
    BEGIN
        TRUNCATE TABLE silver.crm_cust_info;

        INSERT INTO silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
        SELECT
            a.cst_id,
            a.cst_key,
            TRIM(a.cst_firstname),
            TRIM(a.cst_lastname),
            CASE WHEN UPPER(TRIM(a.cst_marital_status)) = 'S' THEN 'Single'
                 WHEN UPPER(TRIM(a.cst_marital_status)) = 'M' THEN 'Married'
                 ELSE 'N/A' END,
            CASE WHEN UPPER(TRIM(a.cst_gndr)) = 'F' THEN 'Female'
                 WHEN UPPER(TRIM(a.cst_gndr)) = 'M' THEN 'Male'
                 ELSE 'N/A' END,
            a.cst_create_date
        FROM (
            SELECT *
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
                FROM bronze.crm_cust_info
            ) sub
            WHERE rn = 1 AND cst_id IS NOT NULL
        ) a
        JOIN silver.erp_loc_info l ON a.cst_key = l.cid
        JOIN silver.erp_cust_info c ON a.cst_key = c.cid
        WHERE a.cst_key NOT IN (
            SELECT record_key
            FROM monitoring.data_validation_logs
            WHERE table_name = 'crm_cust_info'
            and record_key is not NULL
        );

        GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;
        v_end_time := clock_timestamp();

        INSERT INTO silver.load_log(table_name, load_status, load_time, load_start_time, load_end_time, rows_loaded)
        VALUES ('crm_cust_info', 'SUCCESS', v_end_time - v_start_time, v_start_time, v_end_time, v_rows_inserted);

    EXCEPTION WHEN OTHERS THEN
        v_end_time := clock_timestamp();
        INSERT INTO silver.load_log(table_name, load_status, load_time, load_start_time, load_end_time, rows_loaded, error_message)
        VALUES ('crm_cust_info', 'FAIL', v_end_time - v_start_time, v_start_time, v_end_time, 0, SQLERRM);
        RETURN;
    END;
END;
$$;

-- Procedure to load crm_prd_info
CREATE OR REPLACE PROCEDURE silver.insert_crm_prd_info()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp();
    v_end_time TIMESTAMP;
    v_rows_inserted INTEGER;
BEGIN
    BEGIN
        TRUNCATE TABLE silver.crm_prd_info;

        INSERT INTO silver.crm_prd_info (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
        SELECT
            prd_id,
            SUBSTR(prd_key, 1, 5),
            SUBSTR(prd_key, 7),
            prd_nm,
            COALESCE(prd_cost, 0),
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'N/A'
            END,
            CAST(prd_start_dt AS DATE),
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE)
        FROM bronze.crm_prd_info
        WHERE SUBSTR(prd_key, 1, 5) NOT IN (
            SELECT record_key
            FROM monitoring.data_validation_logs
            WHERE table_name = 'crm_prd_info'
        );

        GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;
        v_end_time := clock_timestamp();

        INSERT INTO silver.load_log(table_name, load_status, load_time, load_start_time, load_end_time, rows_loaded)
        VALUES ('crm_prd_info', 'SUCCESS', v_end_time - v_start_time, v_start_time, v_end_time, v_rows_inserted);

    EXCEPTION WHEN OTHERS THEN
        v_end_time := clock_timestamp();
        INSERT INTO silver.load_log(table_name, load_status, load_time, load_start_time, load_end_time, rows_loaded, error_message)
        VALUES ('crm_prd_info', 'FAIL', v_end_time - v_start_time, v_start_time, v_end_time, 0, SQLERRM);
        RETURN;
    END;
END;
$$;

-- Procedure to load crm_sales_details
CREATE OR REPLACE PROCEDURE silver.insert_crm_sales_details()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp();
    v_end_time TIMESTAMP;
    v_rows_inserted INTEGER;
BEGIN
    BEGIN
        TRUNCATE TABLE silver.crm_sales_details;

        INSERT INTO silver.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt,
            sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
        )
        SELECT DISTINCT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS TEXT)) <> 8 THEN NULL ELSE TO_DATE(CAST(sls_order_dt AS TEXT), 'YYYYMMDD') END,
            CASE WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS TEXT)) <> 8 THEN NULL ELSE TO_DATE(CAST(sls_ship_dt AS TEXT), 'YYYYMMDD') END,
            CASE WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS TEXT)) <> 8 THEN NULL ELSE TO_DATE(CAST(sls_due_dt AS TEXT), 'YYYYMMDD') END,
            CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales <> sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price) ELSE sls_sales END,
            sls_quantity,
            CASE WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0) ELSE sls_price END
        FROM bronze.crm_sales_details
        WHERE sls_ord_num NOT IN (
            SELECT record_key
            FROM monitoring.data_validation_logs
            WHERE table_name = 'crm_sales_details'
        );

        GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;
        v_end_time := clock_timestamp();

        INSERT INTO silver.load_log(table_name, load_status, load_time, load_start_time, load_end_time, rows_loaded)
        VALUES ('crm_sales_details', 'SUCCESS', v_end_time - v_start_time, v_start_time, v_end_time, v_rows_inserted);

    EXCEPTION WHEN OTHERS THEN
        v_end_time := clock_timestamp();
        INSERT INTO silver.load_log(table_name, load_status, load_time, load_start_time, load_end_time, rows_loaded, error_message)
        VALUES ('crm_sales_details', 'FAIL', v_end_time - v_start_time, v_start_time, v_end_time, 0, SQLERRM);
        RETURN;
    END;
END;
$$;
