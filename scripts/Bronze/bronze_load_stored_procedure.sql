/*
===============================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================================
Script Purpose:
	This stored procedure loads data into the "bronze" schema from external CSV files.
	It performs the following actions:
	-Truncates the bronze tables before loading data.
	-Uses the `BULK INSERT` command to laod data from CSV files to bronze tables.

	Parameters:
			None.
		This stored procedure does not accept any parameters or return any values.

	Usage Example:
		EXEC bronze.load_bronze;
==============================================================================================
*/

USE DataWarehouse;
GO
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @bronze_start_time DATETIME, @bronze_end_time DATETIME;
	DECLARE @start_time DATETIME, @end_time DATETIME;

	SET @bronze_start_time = GETDATE();
	BEGIN TRY
		PRINT '============================================================';
		PRINT 'Loading Bronze Layer'
		PRINT '============================================================';

		PRINT '------------------------------------------------------------';
		PRINT 'Loading CRM Table';
		PRINT '------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;


		PRINT '>> Inserting Data Into: crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\daniel\Desktop\Serious Projects\data warehouse projects\crm_and_erp_data_warehouse\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into Table: crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\daniel\Desktop\Serious Projects\data warehouse projects\crm_and_erp_data_warehouse\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------------'


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into Table: crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\daniel\Desktop\Serious Projects\data warehouse projects\crm_and_erp_data_warehouse\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------------'

		PRINT '------------------------------------------------------------';
		PRINT 'Loading ERP Table';
		PRINT '------------------------------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into Table: erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\daniel\Desktop\Serious Projects\data warehouse projects\crm_and_erp_data_warehouse\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------------'

		SET @start_time = GETDATE();

		PRINT '>> Truncating Table: erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into Table: erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\daniel\Desktop\Serious Projects\data warehouse projects\crm_and_erp_data_warehouse\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into Table: erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\daniel\Desktop\Serious Projects\data warehouse projects\crm_and_erp_data_warehouse\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------------'

	END TRY
	BEGIN CATCH
		PRINT '========================================='
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST (ERROR_NUMBER() AS NVARCHAR); 
		PRINT 'Error Number: ' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '========================================='
	END CATCH

	SET @bronze_end_time = GETDATE();
	PRINT '======================================================================';
	PRINT 'Total Load Duration:' + CAST(DATEDIFF(second, @bronze_start_time, @bronze_end_time) AS NVARCHAR) + ' seconds';
	PRINT '======================================================================';
END
