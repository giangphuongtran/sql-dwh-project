CREATE OR REPLACE PROCEDURE silver.insert_erp_px_cat()
LANGUAGE plpgsql
AS $$
BEGIN
    BEGIN
        TRUNCATE TABLE silver.erp_px_cat;
        INSERT INTO silver.erp_px_cat (id, cat, subcat, maintenance)
        SELECT
            REPLACE("ID", '_', '-') AS id,
            TRIM("CAT"),
            TRIM("SUBCAT"),
            TRIM("MAINTENANCE")
        FROM bronze.erp_px_cat;
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO silver.load_log(table_name, load_status, error_message)
        VALUES ('erp_px_cat', 'FAIL', SQLERRM);
        RETURN;
    END;
    INSERT INTO silver.load_log(table_name, load_status)
    VALUES ('erp_px_cat', 'SUCCESS');
END;
$$;

CREATE OR REPLACE PROCEDURE silver.insert_erp_loc_info()
LANGUAGE plpgsql
AS $$
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
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO silver.load_log(table_name, load_status, error_message)
        VALUES ('erp_loc_info', 'FAIL', SQLERRM);
        RETURN;
    END;
    INSERT INTO silver.load_log(table_name, load_status)
    VALUES ('erp_loc_info', 'SUCCESS');
END;
$$;

CREATE OR REPLACE PROCEDURE silver.insert_erp_cust_info()
LANGUAGE plpgsql
AS $$
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
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO silver.load_log(table_name, load_status, error_message)
        VALUES ('erp_cust_info', 'FAIL', SQLERRM);
        RETURN;
    END;
    INSERT INTO silver.load_log(table_name, load_status)
    VALUES ('erp_cust_info', 'SUCCESS');
END;
$$;

CREATE OR REPLACE PROCEDURE silver.insert_crm_cust_info()
LANGUAGE plpgsql
AS $$
BEGIN
    BEGIN
        TRUNCATE TABLE silver.crm_cust_info;
        INSERT INTO silver.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname,
            cst_marital_status, cst_gndr, cst_create_date
        )
        SELECT
            a.cst_id,
            a.cst_key,
            TRIM(a.cst_firstname),
            TRIM(a.cst_lastname),
            CASE
                WHEN UPPER(TRIM(a.cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(a.cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'N/A'
            END,
            CASE
                WHEN UPPER(TRIM(a.cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(a.cst_gndr)) = 'M' THEN 'Male'
                ELSE 'N/A'
            END,
            a.cst_create_date
        FROM (
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS FLAG_LATEST
                FROM bronze.crm_cust_info
            ) sub
            WHERE FLAG_LATEST = 1
              AND cst_id IS NOT NULL
        ) a
        JOIN silver.erp_cust_info ec ON a.cst_key = ec.cid
        JOIN silver.erp_loc_info el ON a.cst_key = el.cid;
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO silver.load_log(table_name, load_status, error_message)
        VALUES ('crm_cust_info', 'FAIL', SQLERRM);
        RETURN;
    END;
    INSERT INTO silver.load_log(table_name, load_status)
    VALUES ('crm_cust_info', 'SUCCESS');
END;
$$;

CREATE OR REPLACE PROCEDURE silver.insert_crm_prd_info()
LANGUAGE plpgsql
AS $$
BEGIN
    BEGIN
        TRUNCATE TABLE silver.crm_prd_info;
        INSERT INTO silver.crm_prd_info (
            prd_id, cat_id, prd_key, prd_nm,
            prd_cost, prd_line, prd_start_dt, prd_end_dt
        )
        SELECT
            prd_id,
            SUBSTR(prd_key, 1, 5) AS cat_id,
            SUBSTR(prd_key, 7, LENGTH(prd_key)) AS prd_key,
            prd_nm,
            COALESCE(prd_cost, 0),
            CASE
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                ELSE 'N/A'
            END AS prd_line,
            CAST(prd_start_dt AS DATE),
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE)
        FROM bronze.crm_prd_info
        WHERE prd_id IS NOT NULL
          AND SUBSTR(prd_key, 1, 5) IN (
              SELECT REPLACE(id, '_', '-') FROM silver.erp_px_cat
          );
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO silver.load_log(table_name, load_status, error_message)
        VALUES ('crm_prd_info', 'FAIL', SQLERRM);
        RETURN;
    END;
    INSERT INTO silver.load_log(table_name, load_status)
    VALUES ('crm_prd_info', 'SUCCESS');
END;
$$;

CREATE OR REPLACE PROCEDURE silver.insert_crm_sales_details()
LANGUAGE plpgsql
AS $$
BEGIN
    BEGIN
        TRUNCATE TABLE silver.crm_sales_details;
        INSERT INTO silver.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id,
            sls_order_dt, sls_ship_dt, sls_due_dt,
            sls_sales, sls_quantity, sls_price
        )
        SELECT DISTINCT
            sd.sls_ord_num,
            sd.sls_prd_key,
            sd.sls_cust_id,
            CASE WHEN sd.sls_order_dt = 0 OR LENGTH(CAST(sd.sls_order_dt AS VARCHAR)) <> 8 THEN NULL
                 ELSE CAST(CAST(sd.sls_order_dt AS VARCHAR) AS DATE) END,
            CASE WHEN sd.sls_ship_dt = 0 OR LENGTH(CAST(sd.sls_ship_dt AS VARCHAR)) <> 8 THEN NULL
                 ELSE CAST(CAST(sd.sls_ship_dt AS VARCHAR) AS DATE) END,
            CASE WHEN sd.sls_due_dt = 0 OR LENGTH(CAST(sd.sls_due_dt AS VARCHAR)) <> 8 THEN NULL
                 ELSE CAST(CAST(sd.sls_due_dt AS VARCHAR) AS DATE) END,
            CASE WHEN sd.sls_sales <= 0 OR sd.sls_sales IS NULL
                      OR sd.sls_sales <> sd.sls_quantity * ABS(sd.sls_price)
                 THEN sd.sls_quantity * ABS(sd.sls_price)
                 ELSE sd.sls_sales END,
            sd.sls_quantity,
            CASE WHEN sd.sls_price <= 0 OR sd.sls_price IS NULL
                 THEN sd.sls_sales / NULLIF(sd.sls_quantity, 0)
                 ELSE sd.sls_price END
        FROM bronze.crm_sales_details sd
        JOIN silver.crm_cust_info ci ON sd.sls_cust_id = ci.cst_id
        WHERE NOT EXISTS (
            SELECT 1 FROM silver.crm_sales_details s
            WHERE s.sls_ord_num = sd.sls_ord_num AND s.sls_prd_key = sd.sls_prd_key
        );
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO silver.load_log(table_name, load_status, error_message)
        VALUES ('crm_sales_details', 'FAIL', SQLERRM);
        RETURN;
    END;
    INSERT INTO silver.load_log(table_name, load_status)
    VALUES ('crm_sales_details', 'SUCCESS');
END;
$$;
