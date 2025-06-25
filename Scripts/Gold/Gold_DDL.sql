-- Data integrations and building gold layer
-- getting customer info --
--creating view for customers
CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER(ORDER BY cst_id) AS Customer_key,
	cinfo.cst_id AS Customer_id,
	cinfo.cst_key AS Customer_number,
	cinfo.cst_firstname AS First_name,
	cinfo.cst_lastname AS Last_name,
	loc.cntry AS Country,
	cinfo.cst_marital_status AS marital_status,
	CASE WHEN cinfo.cst_gndr != 'n/a' THEN cinfo.cst_gndr -- treating the CRM as master
		 ELSE COALESCE(ca.gen, 'n/a') -- taking care of nulls(from the join)
	END as gender,
	ca.bdate AS Birthdate,
	cinfo.cst_create_date AS Create_date
	from silver.crm_cust_info cinfo
	LEFT JOIN silver.erp_cust_az12 ca
	on cinfo.cst_key = ca.cid
	left join silver.erp_loc_a101 loc
	on cinfo.cst_key = loc.cid

--check to see if after the joins there was any duplication
SELECT cst_id, COUNT(*) FROM
(SELECT
	cinfo.cst_id,
	cinfo.cst_key,
	cinfo.cst_firstname,
	cinfo.cst_lastname,
	cinfo.cst_marital_status,
	cinfo.cst_gndr,
	cinfo.cst_create_date,
	ca.bdate,
	ca.gen,
	loc.cntry
	from silver.crm_cust_info cinfo
	LEFT JOIN silver.erp_cust_az12 ca
	on cinfo.cst_key = ca.cid
	left join silver.erp_loc_a101 loc
	on cinfo.cst_key = loc.cid)c
GROUP BY cst_id
HAVING COUNT(*) >1

-- making the products view
CREATE VIEW gold.dim_product AS
Select
	ROW_NUMBER() OVER (ORDER BY p.prd_start_dt, p.prd_key) as Product_key,
	p.prd_id AS Product_id,
	p.prd_key AS Product_number,
	p.prd_nm AS Product_name,
	p.cat_id AS Category_id,
	px.cat AS Category_name,
	px.subcat AS Subcategory,
	px.maintenance,
	p.prd_cost AS Cost,
	p.prd_line AS Product_line,
	p.prd_start_dt AS Start_date
	from silver.crm_prd_info p
	LEFT JOIN silver.erp_px_cat_g1v2 px
	ON p.cat_id = px.id
	WHERE prd_end_dt is NULL -- only getting current products


--checking to see if product keys are unique
select prd_key, COUNT(*) FROM(
select
p.prd_id,
p.cat_id,
p.prd_key,
p.prd_nm,
p.prd_cost,
p.prd_line,
p.prd_start_dt,
px.cat,
px.subcat,
px.maintenance
from silver.crm_prd_info p
LEFT JOIN silver.erp_px_cat_g1v2 px
ON p.cat_id = px.id
WHERE prd_end_dt is NULL -- only getting current products
)c GROUP BY prd_key
HAVING COUNT(*)>1 -- check passed


--creating the fact sales view
CREATE VIEW gold.fact_sales AS
select
s.sls_ord_num AS Sales_Order_number,
pr.product_key,
cinfo.Customer_key,
s.sls_order_dt AS Order_date,
s.sls_ship_dt AS Shipping_date,
s.sls_due_dt AS Due_date,
s.sls_sales AS Sale_amt,
s.sls_quantity AS Quantity,
s.sls_price AS Price
from silver.crm_sales_details s
LEFT JOIN gold.dim_product pr
on s.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cinfo
on s.sls_cust_id = cinfo.Customer_id

