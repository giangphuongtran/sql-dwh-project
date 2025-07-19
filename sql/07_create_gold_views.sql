CREATE MATERIALIZED VIEW gold.dim_customer AS
SELECT
	cci.cst_id AS customer_id
	, cci.cst_key AS customer_key
	, cci.cst_firstname AS first_name
	, cci.cst_lastname AS last_name
	, cci.cst_marital_status AS marital_status
	, CASE
		WHEN cci.cst_gndr IS NOT NULL THEN cci.cst_gndr
		WHEN cci.cst_gndr IS NULL
		OR cci.cst_gndr = 'n/a' THEN COALESCE(eci.gen, 'n/a')
	END AS gender
	, eli.cntry AS country
	, eci.bdate AS birth_date
FROM
	silver.crm_cust_info cci
LEFT JOIN silver.erp_cust_info eci
ON
	cci.cst_key = eci.cid
LEFT JOIN silver.erp_loc_info eli
ON
	cci.cst_key = eli.cid
;

CREATE MATERIALIZED VIEW gold.dim_product AS
SELECT
	cpi.prd_id AS product_id
	, cpi.prd_key AS product_key
	, cpi.prd_nm AS product_name
	, cpi.prd_cost AS product_cost
	, cpi.prd_line AS product_line
	, cpi.cat_id AS category_id
	, epc.cat AS category_name
	, epc.subcat AS sub_category_name
	, epc.maintenance
	, cpi.prd_start_dt AS product_start_date
FROM
	silver.crm_prd_info cpi
LEFT JOIN silver.erp_px_cat epc
ON
	cpi.cat_id = epc.id
WHERE
	cpi.prd_end_dt IS NULL
;

CREATE MATERIALIZED VIEW gold.fact_sales AS
SELECT
	csd.sls_ord_num AS order_number
	, dp.product_key
	, dc.customer_id
	, csd.sls_order_dt AS order_date
	, csd.sls_ship_dt AS ship_date
	, csd.sls_due_dt AS due_date
	, csd.sls_sales AS sales_amount
	, csd.sls_quantity AS quantity
	, csd.sls_price AS price
FROM
	silver.crm_sales_details csd
LEFT JOIN gold.dim_customer dc
ON
	csd.sls_cust_id = dc.customer_id
LEFT JOIN gold.dim_product dp
ON
	csd.sls_prd_key = dp.product_key