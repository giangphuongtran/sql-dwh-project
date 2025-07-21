-- Create the second layer of the Medallion architecture
CREATE SCHEMA IF NOT EXISTS silver;

-- silver.erp_loc_info
DROP TABLE IF EXISTS silver.erp_loc_info CASCADE;
CREATE TABLE silver.erp_loc_info (
    cid TEXT NOT NULL,
    cntry TEXT,
    updated_dttm DATE NOT NULL DEFAULT current_date,
    CONSTRAINT pk_erp_loc PRIMARY KEY (cid)
);

-- silver.erp_cust_info
DROP TABLE IF EXISTS silver.erp_cust_info CASCADE;
CREATE TABLE silver.erp_cust_info (
    cid TEXT NOT NULL,
    bdate DATE,
    gen TEXT,
    updated_dttm DATE NOT NULL DEFAULT current_date,
    CONSTRAINT pk_erp_cust PRIMARY KEY (cid)
--    ,
--    CONSTRAINT fk_erp_cid FOREIGN KEY (cid) REFERENCES silver.erp_loc_info (cid)
);

-- silver.erp_px_cat
DROP TABLE IF EXISTS silver.erp_px_cat CASCADE;
CREATE TABLE silver.erp_px_cat (
    id TEXT NOT NULL,
    cat TEXT,
    subcat TEXT,
    maintenance TEXT,
    updated_dttm DATE NOT NULL DEFAULT current_date,
    CONSTRAINT pk_cat_id PRIMARY KEY (id)
);

-- silver.crm_prd_info
DROP TABLE IF EXISTS silver.crm_prd_info CASCADE;
CREATE TABLE silver.crm_prd_info (
    prd_id INT NOT NULL,
    cat_id TEXT,
    prd_key TEXT,
    prd_nm TEXT,
    prd_cost INT,
    prd_line TEXT,
    prd_start_dt DATE,
    prd_end_dt DATE,
    updated_dttm DATE NOT NULL DEFAULT current_date,
    CONSTRAINT pk_crm_prd PRIMARY KEY (prd_id)
--    ,
--    CONSTRAINT fk_cat_id FOREIGN KEY (cat_id) REFERENCES silver.erp_px_cat (id)
);

-- silver.crm_cust_info
DROP TABLE IF EXISTS silver.crm_cust_info CASCADE;
CREATE TABLE silver.crm_cust_info (
    cst_id INT NOT NULL,
    cst_key TEXT,
    cst_firstname TEXT,
    cst_lastname TEXT,
    cst_marital_status TEXT,
    cst_gndr TEXT,
    cst_create_date DATE,
    updated_dttm DATE NOT NULL DEFAULT current_date,
    CONSTRAINT pk_crm_cust PRIMARY KEY (cst_id)
--    ,
--    CONSTRAINT fk_cust_loc FOREIGN KEY (cst_key) REFERENCES silver.erp_loc_info (cid),
--    CONSTRAINT fk_cust_cid FOREIGN KEY (cst_key) REFERENCES silver.erp_cust_info (cid)
);

-- silver.crm_sales_details
DROP TABLE IF EXISTS silver.crm_sales_details CASCADE;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num TEXT NOT NULL,
    sls_prd_key TEXT,
    sls_cust_id INT,
    sls_order_dt date,
    sls_ship_dt date,
    sls_due_dt date,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    updated_dttm DATE NOT NULL DEFAULT current_date,
    CONSTRAINT pk_sls_details PRIMARY KEY (sls_ord_num, sls_prd_key)
--    ,
--    CONSTRAINT fk_sales_cust FOREIGN KEY (sls_cust_id) REFERENCES silver.crm_cust_info (cst_id)
);

CREATE TABLE IF NOT EXISTS silver.load_log (
    log_id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    load_status TEXT NOT NULL CHECK (load_status IN ('SUCCESS', 'FAIL')),
    load_time INTERVAL,
    load_start_time TIMESTAMP NOT null,
    load_end_time TIMESTAMP NOT null,
    rows_loaded INT,
    error_message TEXT
);