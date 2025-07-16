-- Create all layers of the Medallion architecture
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- bronze.crm_cust_info definition

-- Drop table

-- DROP TABLE bronze.crm_cust_info;

CREATE TABLE IF NOT EXISTS bronze.crm_cust_info (
	cst_id int4 NULL,
	cst_key text NULL,
	cst_firstname text NULL,
	cst_lastname text NULL,
	cst_marital_status text NULL,
	cst_gndr text NULL,
	cst_create_date date NULL,
	import_date date NOT NULL,
	source_file text NOT NULL
);


-- bronze.crm_prd_info definition

-- Drop table

-- DROP TABLE bronze.crm_prd_info;

CREATE TABLE IF NOT EXISTS bronze.crm_prd_info (
	prd_id int4 NULL,
	prd_key text NULL,
	prd_nm text NULL,
	prd_cost int4 NULL,
	prd_line text NULL,
	prd_start_dt date NULL,
	prd_end_dt date NULL,
	import_date date NOT NULL,
	source_file text NOT NULL
);


-- bronze.crm_sales_details definition

-- Drop table

-- DROP TABLE bronze.crm_sales_details;

CREATE TABLE IF NOT EXISTS bronze.crm_sales_details (
	sls_ord_num text NULL,
	sls_prd_key text NULL,
	sls_cust_id int4 NULL,
	sls_order_dt int4 NULL,
	sls_ship_dt int4 NULL,
	sls_due_dt int4 NULL,
	sls_sales int4 NULL,
	sls_quantity int4 NULL,
	sls_price int4 NULL,
	import_date date NOT NULL,
	source_file text NOT NULL
);


-- bronze.erp_cust_info definition

-- Drop table

-- DROP TABLE bronze.erp_cust_info;

CREATE TABLE IF NOT EXISTS bronze.erp_cust_info (
	"CID" text NULL,
	"BDATE" date NULL,
	"GEN" text NULL,
	import_date date NOT NULL,
	source_file text NOT NULL
);


-- bronze.erp_loc_info definition

-- Drop table

-- DROP TABLE bronze.erp_loc_info;

CREATE TABLE IF NOT EXISTS bronze.erp_loc_info (
	"CID" text NULL,
	"CNTRY" text NULL,
	import_date date NOT NULL,
	source_file text NOT NULL
);


-- bronze.erp_px_cat definition

-- Drop table

-- DROP TABLE bronze.erp_px_cat;

CREATE TABLE IF NOT EXISTS bronze.erp_px_cat (
	"ID" text NULL,
	"CAT" text NULL,
	"SUBCAT" text NULL,
	"MAINTENANCE" text NULL,
	import_date date NOT NULL,
	source_file text NOT NULL
);