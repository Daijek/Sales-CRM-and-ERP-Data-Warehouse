/*
===============================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================================
Script Purpose:
	This stored procedure cleans and loads data into the "silver" schema the bronze layer.
	It performs the following actions:
	-Truncates the silver tables before loading data.
	-Uses the `INSERT INTO` command to clean and laod data from bronze layer  to silver layer.

	Parameters:
			None.
		This stored procedure does not accept any parameters or return any values.

	Usage Example:
		EXEC silver.load_silver;
==============================================================================================
*/



CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	BEGIN TRY
		DECLARE 
			@start_time DATETIME, 
			@end_time DATETIME,
			@overall_start_time DATETIME, 
			@overall_end_time DATETIME;
		PRINT '>> Truncating table: silver.[crm_cust_info]'
		TRUNCATE TABLE [DataWarehouse].[silver].[crm_cust_info]
		PRINT '>> Inserting Data Into: silver.[crm_cust_info]'
		
		SET @overall_start_time = GETDATE()
		SET @start_time = GETDATE()
		INSERT INTO silver.crm_cust_info (
			[cst_id],
			[cst_key],
			[cst_firstname],
			[cst_lastname],
			[cst_marital_status],
			[cst_gndr],
			[cst_create_date])
	

		SELECT 
			[cst_id],
			[cst_key],
			TRIM([cst_firstname]) AS [cst_firstname],
			TRIM([cst_lastname]) AS [cst_lastname],
			CASE 
				WHEN TRIM(UPPER([cst_marital_status])) = 'S' THEN 'Single'
				WHEN TRIM(UPPER([cst_marital_status])) = 'M' THEN 'Married'
				ELSE 'N/A'
			END [cst_marital_status],

			CASE
				WHEN TRIM(UPPER([cst_gndr])) = 'M' THEN 'Male'
				WHEN TRIM(UPPER([cst_gndr])) = 'F' THEN 'Female'
				ELSE 'N/A'
			END [cst_gndr],
			[cst_create_date]

		FROM (
			SELECT *,
				-- ranking the cst_id column by create data
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM [DataWarehouse].[bronze].[crm_cust_info]
			WHERE cst_id IS NOT NULL
		)t

		WHERE flag_last = 1
		SET @end_time = GETDATE();
		PRINT 'load time: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds' 
		PRINT '====================================================================='
		PRINT ''
		PRINT ''


		SET @start_time = GETDATE()
		PRINT '>> Truncating table: silver.[crm_prd_info]'
		TRUNCATE TABLE [DataWarehouse].[silver].[crm_prd_info]
		PRINT '>> Inserting Data Into: silver.[crm_prd_info]'

		INSERT INTO [DataWarehouse].[silver].[crm_prd_info](
			[prd_id],
			[cat_id],
			[prd_key],
			[prd_nm],
			[prd_cost],
			[prd_line],
			[prd_start_dt],
			[prd_end_dt]
		)

		SELECT 
			[prd_id],
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
			REPLACE(SUBSTRING(prd_key, 7, LEN(prd_key)-6), '_', '-') AS prd_key,
			[prd_nm],
			ISNULL([prd_cost], 0) AS prd_cost, -- Replacing Null values in product cost with 0
			CASE TRIM(UPPER([prd_line]))
				WHEN 'M' then 'Mountain'
				WHEN 'R' then 'Road'
				WHEN 'S' then 'Other Sales'
				WHEN 'T' then 'Touring'
				ELSE 'n/a'
			END [prd_line],
			CAST([prd_start_dt] AS DATE) AS [prd_start_dt],
			CAST(LEAD([prd_start_dt]) OVER (PARTITION BY [prd_key] ORDER BY [prd_start_dt] - 1 ASC) AS DATE) AS [prd_end_dt]
		FROM [DataWarehouse].[bronze].[crm_prd_info]
	
		SET @end_time = GETDATE();

		PRINT 'load time: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds' 
		PRINT '====================================================================='
		PRINT ''
		PRINT ''


		PRINT '>> Truncating table: silver.[[crm_sales_details]]'
		TRUNCATE TABLE [DataWarehouse].[silver].[crm_sales_details]
		PRINT '>> Inserting Data Into: silver.[[crm_sales_details]]'
		SET @start_time = GETDATE()
		INSERT INTO [DataWarehouse].[silver].[crm_sales_details](
			[sls_ord_num],
			[sls_prd_key],
			[sls_cust_id],
			[sls_order_dt],
			[sls_ship_dt],
			[sls_due_dt],
			[sls_sales],
			[sls_quantity],
			[sls_price]
		)

		SELECT
			[sls_ord_num],
			[sls_prd_key],
			[sls_cust_id],
			CASE
				WHEN [sls_order_dt] <=0 OR LEN(NULLIF([sls_order_dt],0)) != 8 THEN NULL
				ELSE CAST(CAST([sls_order_dt] AS NVARCHAR(50)) AS DATE)
			END [sls_order_dt],
			CASE
				WHEN [sls_ship_dt] <=0 OR LEN(NULLIF([sls_ship_dt],0)) != 8 THEN NULL
				ELSE CAST(CAST([sls_ship_dt] AS NVARCHAR(50)) AS DATE)
			END [sls_ship_dt],
			CASE
				WHEN [sls_due_dt] <=0 OR LEN(NULLIF([sls_due_dt],0)) != 8 THEN NULL
				ELSE CAST(CAST([sls_due_dt] AS NVARCHAR(50)) AS DATE)
			END [sls_due_dt],
			CASE
				WHEN [sls_sales] IS NULL OR 
					[sls_sales] <= 0 OR 
					ABS([sls_price]) * ABS([sls_quantity]) != [sls_sales] AND
					[sls_price] IS NOT NULL AND
					[sls_quantity] IS NOT NULL
				THEN ABS([sls_quantity]) * ABS([sls_price])

				ELSE ABS([sls_sales])
			END [sls_sales],
			[sls_quantity],
			CASE 
				WHEN [sls_price] IS NULL OR [sls_price] <= 0 AND
					ABS([sls_sales]) IS NOT NULL AND
					ABS([sls_quantity]) IS NOT NULL 
				THEN [sls_sales]/NULLIF([sls_quantity],0)
				ELSE ABS([sls_price])
			END [sls_price]

		FROM [DataWarehouse].[bronze].[crm_sales_details]

		SET @end_time = GETDATE();

		PRINT 'load time: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds' 
		PRINT '====================================================================='
		PRINT ''
		PRINT ''



		PRINT '>> Truncating table: silver.[erp_cust_az12]'
		TRUNCATE TABLE [DataWarehouse].[silver].[erp_cust_az12]
		PRINT '>> Inserting Data Into: silver.[erp_cust_az12]'
		SET @start_time = GETDATE()
		INSERT INTO [DataWarehouse].[silver].[erp_cust_az12](
			[cid],
			[bdate],
			[gen]
		)

		SELECT 
			-- The vesion of cid that we have in other tables does not have the first 3 letters so we fix that
			CASE 
				WHEN LEN([cid]) = 13 THEN SUBSTRING([cid], 4, LEN([cid]))
				ELSE [cid]
			END [cid],
			-- Removing any birtdates after the current date, because it is not possible
			CASE
				WHEN [bdate] > GETDATE() THEN NULL
				ELSE [bdate]
			END [bdate],
			-- Standardizing the data in the gender column
			CASE 
				WHEN TRIM(UPPER([gen])) IN ('F', 'FEMALE' )THEN 'Female'
				WHEN TRIM(UPPER([gen])) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END [gen]
		FROM [DataWarehouse].[bronze].[erp_cust_az12]

		SET @end_time = GETDATE();

		PRINT 'load time: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds' 
		PRINT '====================================================================='
		PRINT ''
		PRINT ''

		PRINT '>> Truncating table: silver.erp_loc_a101'
		TRUNCATE TABLE [DataWarehouse].[silver].[erp_loc_a101]
		PRINT '>> Inserting Data Into: silver.[erp_loc_a101]'
		SET @start_time = GETDATE()
		INSERT INTO [DataWarehouse].[silver].erp_loc_a101(
			[cid],
			[cntry]
		)

		SELECT
			REPLACE([cid], '-', '') [cid],
			ISNULL(
				CASE UPPER(TRIM([cntry]))
					WHEN 'DE' THEN 'Germany'
					WHEN 'USA' THEN 'United States'
					WHEN 'US' THEN 'United States'
					WHEN '' THEN 'n/a' 
					ELSE [cntry]
				END,
				'n/a'
			) [cntry]

		FROM [DataWarehouse].[bronze].[erp_loc_a101]

		SET @end_time = GETDATE();

		PRINT 'load time: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds' 
		PRINT '====================================================================='
		PRINT ''
		PRINT ''



		PRINT '>> Truncating table: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE [DataWarehouse].[silver].[erp_px_cat_g1v2]
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2'
		SET @start_time = GETDATE()
		INSERT INTO [DataWarehouse].[silver].[erp_px_cat_g1v2](
			[id],
			[cat],
			[subcat],
			[maintenance]
		)
		SELECT 
			[id],
			TRIM([cat]),
			TRIM([subcat]),
			TRIM([maintenance])
		FROM [DataWarehouse].[bronze].[erp_px_cat_g1v2]

		SET @end_time = GETDATE();

		PRINT 'load time: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds' 
		PRINT '====================================================================='
		PRINT ''
		PRINT ''

		SET @overall_end_time = GETDATE()
		PRINT 'overall load time: ' + CAST(DATEDIFF(second, @overall_start_time, @overall_end_time) AS NVARCHAR) + ' seconds' 
		PRINT '====================================================================='

	END TRY
	BEGIN CATCH
		PRINT '========================================='
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST (ERROR_NUMBER() AS NVARCHAR); 
		PRINT 'Error Number: ' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '========================================='
	END CATCH
END