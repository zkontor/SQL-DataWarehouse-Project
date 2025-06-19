CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '--------------------------------';
		PRINT 'Loading Bronze Layer';
		PRINT '--------------------------------';
		--wanting to truncate then insert
		PRINT '--------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Zach\Documents\GitHub\SQL-DataWarehouse-Project\Datasets\soruce_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) +'Seconds';

		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Zach\Documents\GitHub\SQL-DataWarehouse-Project\Datasets\soruce_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) +'Seconds';

		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Zach\Documents\GitHub\SQL-DataWarehouse-Project\Datasets\soruce_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) +'Seconds';

		PRINT '--------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '--------------------------------';

		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Zach\Documents\GitHub\SQL-DataWarehouse-Project\Datasets\soruce_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) +'Seconds';

		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Zach\Documents\GitHub\SQL-DataWarehouse-Project\Datasets\soruce_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) +'Seconds';

		SET @end_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Zach\Documents\GitHub\SQL-DataWarehouse-Project\Datasets\soruce_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) +'Seconds';
		PRINT'--------';
		SET @batch_end_time = GETDATE();
		Print'--------------';
		PRINT'Bronze Layer loading is complete';
		PRINT' Total Duration of this batch load:' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds'; 
	END TRY
	BEGIN CATCH
		PRINT '------------------'
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '------------------'
	END CATCH
END
GO


EXEC bronze.load_bronze;
