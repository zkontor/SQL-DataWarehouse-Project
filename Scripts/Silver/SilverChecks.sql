-- checking for duplicate PK
select cst_id, COUNT(*) as PKCount
from bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 or cst_id IS NULL;

--checks
select
*
FROM bronze.crm_cust_info 
where cst_id = 29466;

-- picking the latest one and using the rank
SELECT
*
FROM(
	select
	*,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as Flag
	FROM bronze.crm_cust_info 
	where cst_id IS NOT NULL)
	as flagSub
	WHERE Flag = 1

-- checking for unwanted spaces in str columns
select cst_firstname
from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname)

select * from silver.crm_cust_info;


--- now checking prod table
 

 --checking for nulls or negative numbers in prd_nm
 -- should not have any
 select prd_cost
 from bronze.crm_prd_info
 where prd_cost < 0 or prd_cost IS NULL

 --checks for distinct product lines
 select distinct prd_line
 from bronze.crm_prd_info

 -- check for invalid orders (end date cannot be before start date)
 select * from bronze.crm_prd_info
 where prd_end_dt < prd_start_dt

 -- needing to use the end date for the previous record(-1 day) (for no overlap) as the start date for the next record for the value of start_dt or Null for the current cost of the product information

select
 prd_id,
 prd_key,
 prd_nm,
 prd_start_dt,
 prd_end_dt,
 LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) -1 prd_start_dt_test
 from bronze.crm_prd_info
 where prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509')

 	-- prd_info cleaning and transformations:
select
 prd_id,
 REPLACE(SUBSTRING(prd_key, 1,5),'-','_') as cat_id, 
 SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key, 
 prd_key,
 prd_nm,
 ISNULL(prd_cost, 0) as prd_cost,
 CASE UPPER(TRIM(prd_line)) 
	WHEN 'M'THEN 'Mountian'
	WHEN 'R'THEN 'Road'
	WHEN 'S'THEN 'Other Sales'
	WHEN 'T'THEN 'Touring'
	ELSE 'n/a'
END as prd_line, 
CAST(prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_start_dt
from bronze.crm_prd_info

select * from silver.crm_prd_info

-- Sales details cleaning and transformations:
select
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 or LEN(sls_order_dt) != 8 THEN Null
	else CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END sls_order_dt,
CASE WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN Null
	else CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END sls_ship_dt,
CASE WHEN sls_due_dt = 0 or LEN(sls_due_dt) != 8 THEN Null
	else CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END sls_due_dt,
 CASE WHEN sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE WHEN sls_price is null or sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity,0)
	else sls_price
END AS sls_price
from bronze.crm_sales_details


-- data quality checks for sls_sales_details
select 
NULLIF (sls_due_dt,0) sls_due_dt
from bronze.crm_sales_details 
where sls_due_dt <=0 or LEN(sls_due_dt) !=8

--checking to make sure order comes before ship(logical check) 

 select * from silver.crm_sales_details
 where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

 -- Sales/price Checks:
 select DISTINCT
 sls_sales,
 sls_quantity,
 sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales IS NULL or sls_quantity is NULL or sls_price IS NULL
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales, sls_quantity,sls_price 

-- Final check
select * from silver.crm_sales_details



--cleaning and tranforming erp_cust_az12 table
select
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	else cid
END AS cid,
CASE WHEN bdate > GETDATE() THEN NULL
	else bdate
END AS bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
	else 'n/a'
END AS gen
from bronze.erp_cust_az12


-- Cleaning erp_loc_a101 table
select 
REPLACE(cid,'-','') cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	WHEN TRIM(cntry) = '' or cntry IS NULL then 'n/a'
	else TRIM(cntry)
END AS cntry
from bronze.erp_loc_a101


--Cleaning cat_g1v2 table
select
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2 -- quality is good no cleaning needed


-- check





