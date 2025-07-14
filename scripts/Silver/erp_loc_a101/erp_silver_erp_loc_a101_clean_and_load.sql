PRINT '>> Truncating table: silver.erp_loc_a101'
TRUNCATE TABLE [DataWarehouse].[silver].[erp_loc_a101]
PRINT '>> Inserting Data Into: silver.[erp_loc_a101]'

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