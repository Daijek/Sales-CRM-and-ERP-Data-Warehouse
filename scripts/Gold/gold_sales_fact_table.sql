CREATE OR ALTER VIEW [gold].[fact_sales] AS
	SELECT
		[sd].[sls_ord_num] AS [order_number],
		[dp].[product_key] AS [product_key],
		[dc].[customer_key] AS [customer_key],
		[sd].[sls_order_dt] AS [order_date],
		[sd].[sls_ship_dt] AS [shipping_date],
		[sd].[sls_due_dt] AS [due_date],
		[sd].[sls_sales] AS [sales_amount],
		[sd].[sls_quantity] AS [quantity],
		[sd].[sls_price] AS [price]
	

	FROM [DataWarehouse].[silver].[crm_sales_details] AS [sd]
	LEFT JOIN [DataWarehouse].[gold].[dim_customers] AS [dc]
		ON [sd].[sls_cust_id] = [dc].[customer_id]
	LEFT JOIN [DataWarehouse].[gold].[dim_products] [dp]
		ON [sd].[sls_prd_key] = [dp].[product_number]

