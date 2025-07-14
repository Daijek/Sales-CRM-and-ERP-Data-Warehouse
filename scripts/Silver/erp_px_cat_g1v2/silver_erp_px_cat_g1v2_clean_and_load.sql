PRINT '>> Truncating table: silver.erp_px_cat_g1v2'
TRUNCATE TABLE [DataWarehouse].[silver].[erp_px_cat_g1v2]
PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2'
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