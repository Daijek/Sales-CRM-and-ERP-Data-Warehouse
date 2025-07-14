-- Check overall quality
SELECT
	[cid],
	[cntry]
FROM [DataWarehouse].[silver].[erp_loc_a101]

-- Check for unwanted spaces in string columns
SELECT [cid] 
FROM [DataWarehouse].[silver].[erp_loc_a101]
WHERE TRIM([cid]) != [cid]

SELECT [cntry] 
FROM [DataWarehouse].[silver].[erp_loc_a101]
WHERE TRIM([cntry]) != [cntry]

-- Check the country column, because it has low cardinality
SELECT DISTINCT
	ISNULL(
		CASE UPPER(TRIM([cntry]))
			WHEN 'DE' THEN 'Denmark'
			WHEN 'USA' THEN 'United States'
			WHEN 'US' THEN 'United States'
			WHEN '' THEN 'n/a' 
			ELSE [cntry]
		END,
		'n/a'
	) [cntry]
FROM [DataWarehouse].[silver].[erp_loc_a101]

