/*
-------------------------------------------------------
Stored Procedure: Load Silver Layer (Bronze to Silver)
-------------------------------------------------------
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver'tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleaned data from Bronze into Silver tables.
Usage Example:
    EXEC Silver.load_silver;
-------------------------------------------------------
*/ 
Create or ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '------------------------------------------------';
        PRINT 'Loading Silver Layer';
        PRINT '------------------------------------------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';
		--loading the silver.crm_cust_info
		set @start_time = GETDATE();
		Print '--Truncating Table: silver.crm_cust_info--'; 
		TRUNCATE TABLE silver.crm_cust_info;
		Print '--Inserting Data Into: silver.crm_cust_info--'; 
		INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)

		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,
		CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' then 'Single'
			WHEN UPPER(TRIM(cst_material_status)) = 'M' then 'Married'
			ELSE 'n/a'
			END cst_material_status, -- normalized marital status
		CASE WHEN UPPER(TRIM(cst_gender)) = 'F' then 'Female'
			WHEN UPPER(TRIM(cst_gender)) = 'M' then 'Male'
			ELSE 'n/a'
			END cst_gender, -- normalized gender
		cst_create_date
		FROM (
			select
			*,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as Flag
			FROM bronze.crm_cust_info 
			where cst_id IS NOT NULL)
			as flagSub
			WHERE Flag = 1
			SET @end_time = GETDATE();
			PRINT '-- Load Duration Time is: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT ' -------------';


		-- Inserting into silver prd layer post cleaning and transformations:
		SET @start_time = GETDATE();
		Print '--Truncating Table: silver.crm_prd_info--'; 
		TRUNCATE TABLE silver.crm_prd_info;
		Print '--Inserting Data Into: silver.crm_prd_info--';
		INSERT INTO silver.crm_prd_info(
		  prd_id,
		  cat_id,         
		  prd_key,        
		  prd_nm,          
		  prd_cost,        
		  prd_line,        
		  prd_start_dt,
		  prd_end_dt
		  )
		select
		 prd_id,
		 REPLACE(SUBSTRING(prd_key, 1,5),'-','_') as cat_id,-- getting category ID
		 SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,-- extracting Product Key
		 prd_nm,
		 ISNULL(prd_cost, 0) as prd_cost,
		 CASE UPPER(TRIM(prd_line)) 
			WHEN 'M'THEN 'Mountian'
			WHEN 'R'THEN 'Road'
			WHEN 'S'THEN 'Other Sales'
			WHEN 'T'THEN 'Touring'
			ELSE 'n/a'
		END as prd_line, -- mapping the product line codes to descriptive names
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt -- Calculating the end date as one day before the next start date
		from bronze.crm_prd_info
		SET @end_time = GETDATE();
		PRINT '-- Load Duration Time is: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' -------------';

		--insert for sls_sales_details post cleaning and data transformations:
		SET @start_time = GETDATE();
		Print '--Truncating Table: silver.sales_details--'; 
		TRUNCATE TABLE silver.crm_sales_details;
		Print '--Inserting Data Into: silver.sales_details--';
		INSERT INTO silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price)
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
		set @end_time=GETDATE();
		PRINT '-- Load Duration Time is: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' -------------';

		--- insert for erp_cust_az12 table post cleaning
		SET @start_time = GETDATE();
		Print '--Truncating Table: silver.erp_cust_az12--'; 
		TRUNCATE TABLE silver.erp_cust_az12;
		Print '--Inserting Data Into: silver.erp_cust_az12--'
		insert into silver.erp_cust_az12(
		cid,
		bdate,
		gen)
		select
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			else cid
		END AS cid, -- cleaning NAS prefix on cid
		CASE WHEN bdate > GETDATE() THEN NULL
			else bdate
		END AS bdate, --set future bdays to NULL
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
			else 'n/a'
		END AS gen -- normalized the gender values
		from bronze.erp_cust_az12
		SET @end_time = GETDATE();
		PRINT '-- Load Duration Time is: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' -------------';

		--Insert of erp_loc_a101 post cleaning
		SET @start_time = GETDATE();
		Print '--Truncating Table: silver.erp_loc_a101--'; 
		TRUNCATE TABLE silver.erp_loc_a101;
		Print '--Inserting Data Into: silver.erp_loc_a101--'
		insert into silver.erp_loc_a101(
		cid,cntry)
		select 
		REPLACE(cid,'-','') cid,--standardized keys
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(cntry) = '' or cntry IS NULL then 'n/a'
			else TRIM(cntry)
		END AS cntry -- normalized and handeled blank country codes
		from bronze.erp_loc_a101
		set @end_time=GETDATE();
		PRINT '-- Load Duration Time is: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' -------------';


		-- Insert of silver_erp_px_cat_g1v2 table post cleaning
		SET @start_time = GETDATE();
		Print '--Truncating Table: silver.erp_px_cat_g1v2--'; 
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		Print '--Inserting Data Into: silver.erp_px_cat_g1v2--'
		insert into silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance)
		select
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END






