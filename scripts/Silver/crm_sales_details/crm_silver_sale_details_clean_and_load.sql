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

