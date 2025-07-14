-- General Check
SELECT 
	[id],
	[cat],
	[subcat],
	[maintenance]
FROM [DataWarehouse].[silver].[erp_px_cat_g1v2]

-- Check for Nulls and duplicates in id column
-- Expectation: No result
SELECT [id], COUNT(*)
FROM [DataWarehouse].[silver].[erp_px_cat_g1v2]
GROUP BY [id]
HAVING COUNT(*) > 1 OR [id] IS NULL

-- Check for unwanted spaces in string columns
-- Expectation: No result
SELECT [id]
FROM [DataWarehouse].[silver].[erp_px_cat_g1v2]
WHERE TRIM([id]) != [id]

SELECT [cat]
FROM [DataWarehouse].[silver].[erp_px_cat_g1v2]
WHERE TRIM([cat]) != [cat]

SELECT [subcat]
FROM [DataWarehouse].[silver].[erp_px_cat_g1v2]
WHERE TRIM([subcat]) != [subcat]

SELECT [maintenance]
FROM [DataWarehouse].[silver].[erp_px_cat_g1v2]
WHERE TRIM([maintenance]) != [maintenance]

-- Check for low cardinality columns for consistency
SELECT DISTINCT [cat]
FROM [DataWarehouse].[silver].[erp_px_cat_g1v2]

SELECT DISTINCT [subcat]
FROM [DataWarehouse].[silver].[erp_px_cat_g1v2]

SELECT DISTINCT [maintenance]
FROM [DataWarehouse].[silver].[erp_px_cat_g1v2]
