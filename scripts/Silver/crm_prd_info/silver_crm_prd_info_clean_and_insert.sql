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
	REPLACE(SUBSTRING(prd_key, 7, LEN(prd_key)-6), '-', '_') AS prd_key,
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