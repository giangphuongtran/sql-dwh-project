--Clean & load crm_cust_info
-- Check for Nulls or Duplicates
-- Expectation: No result
-- Actual: Some duplicates and also null values exist
select
	cst_id
	,
	count(1) as cnt
from
	bronze.crm_cust_info
group by
	cst_id
having
	count(1) > 1
	or cst_id is null
;
-- Check for unwanted spaces of string columns
-- Expectation: No result
select
	cst_firstname
from
	bronze.crm_cust_info
where
	cst_firstname != TRIM(cst_firstname)
;
-- Data standardization & consistency
select
	distinct cst_gndr
from
	bronze.crm_cust_info
;

select
	distinct cst_marital_status
from
	bronze.crm_cust_info
;
-- Insert data into silver layer
--INSERT INTO silver.crm_cust_info (
--cst_id
--, cst_key
--, cst_firstname
--, cst_lastname
--, cst_marital_status
--, cst_gndr
--, cst_create_date
--)
--SELECT cst_id
--, cst_key
--, TRIM(cst_firstname) AS cst_firstname
--, TRIM(cst_lastname) AS cst_lastname
--, CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
--	WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
--	ELSE 'N/A' END AS cst_marital_status
--, CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
--	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
--	ELSE 'N/A' END AS cst_gndr
--, cst_create_date
--
--FROM (
--SELECT *
--, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS FLAG_LATEST
--FROM bronze.crm_cust_info
--)
--WHERE FLAG_LATEST = 1
--and cst_id is not NULL
--;
--
--DELETE 
--FROM silver.crm_cust_info
;
--Rerun quality check
-- Check for Nulls or Duplicates
-- Expectation: No result
select
	cst_id
	,
	count(1) as cnt
from
	silver.crm_cust_info
group by
	cst_id
having
	count(1) > 1
	or cst_id is null
;
-- Check for unwanted spaces of string columns
-- Expectation: No result
select
	cst_firstname
from
	silver.crm_cust_info
where
	cst_firstname != TRIM(cst_firstname)
;
-- Data standardization & consistency
select
	distinct cst_gndr
from
	silver.crm_cust_info
;

select
	distinct cst_marital_status
from
	silver.crm_cust_info
;

--Clean & load crm_prd_info
-- Check for Nulls or Duplicates
-- Expectation: No result
-- Actual: No result
select
	prd_id
	,
	count(1) as cnt
from
	bronze.crm_prd_info
group by
	prd_id
having
	count(1) > 1
	or prd_id is null
;
-- Check for unwanted spaces of string columns
-- Expectation: No result
select
	cst_firstname
from
	bronze.crm_prd_info
where
	cst_firstname != TRIM(cst_firstname)
;
-- Data standardization & consistency
select
	distinct cst_gndr
from
	bronze.crm_prd_info
;

select
	distinct cst_marital_status
from
	bronze.crm_prd_info
;
--Adjust silver DDL
-- silver.crm_prd_info definition
-- Drop table
DROP TABLE silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
	prd_id int4 NULL,
	cat_id text null,
	prd_key text NULL,
	prd_nm text NULL,
	prd_cost int4 NULL,
	prd_line text NULL,
	prd_start_dt date NULL,
	prd_end_dt date NULL,
	updated_dttm date NOT null default current_date
);
;
-- Insert data into silver layer
insert
	into
	silver.crm_prd_info (
prd_id
,
	prd_key
,
	prd_nm
,
	prd_cost
,
	prd_line
,
	prd_start_dt
,
	prd_end_dt
,
	updated_dttm
)
select
	prd_id
	,
	replace(substr(prd_key, 1, 5), '-', '_') as cat_id
	,
	substr(prd_key, 7, len(prd_key)) as prd_key
	,
	prd_nm
	,
	isnull(prd_cost, 0) as prd_cost
	,
	case
		upper(trim(prd_line))
		when 'M' then 'Mountain'
		when 'R' then 'Road'
		when 'S' then 'Other Sales'
		when 'T' then 'Touring'
		else 'N/A'
	end as prd_line
	,
	cast(prd_start_dt as date) as prd_start_dt
	,
	cast (lead(prd_start_dt) over (partition by prd_key
order by
	prd_start_dt) - 1 as date) as prd_end_dt
	, now()
from
	bronze.crm_prd_info
;
--
--DELETE 
--FROM silver.crm_cust_info
;
--Rerun quality check
-- Check for Nulls or Duplicates
-- Expectation: No result
select
	cst_id
	,
	count(1) as cnt
from
	silver.crm_prd_info
group by
	cst_id
having
	count(1) > 1
	or cst_id is null
;
-- Check for unwanted spaces of string columns
-- Expectation: No result
select
	cst_firstname
from
	silver.crm_prd_info
where
	cst_firstname != TRIM(cst_firstname)
;
-- Data standardization & consistency
select
	distinct cst_gndr
from
	silver.crm_prd_info
;

select
	distinct cst_marital_status
from
	silver.crm_prd_info
;


