PRINT '>> Truncating table: silver.[erp_cust_az12]'
TRUNCATE TABLE [DataWarehouse].[silver].[erp_cust_az12]
PRINT '>> Inserting Data Into: silver.[erp_cust_az12]'

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