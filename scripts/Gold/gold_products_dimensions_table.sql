/*IF OBJECT_ID ('gold.dim_products', 'u') IS NOT NULL
	DROP VIEW [gold].[dim_products];*/

IF EXISTS (SELECT 1 FROM sys.views WHERE object_id = OBJECT_ID(N'[gold].[dim_products]'))
    DROP VIEW [gold].[dim_products];
GO
CREATE VIEW [gold].[dim_products] AS
	SELECT DISTINCT 
		ROW_NUMBER() OVER(ORDER BY [pi].[prd_id]) AS [product_key], -- Adding surrogate key
		[pi].[prd_id] AS [product_id],
		[pi].[prd_key] AS [product_number],
		[pi].[prd_nm] AS [name],
		[pi].[cat_id] AS [category_id],
		[ct].[cat] AS [category],
		[ct].[subcat] AS [subcategory],
		[ct].[maintenance] AS [maintenance],
		[pi].[prd_cost] AS [cost],
		[pi].[prd_line] AS [product_line],
		[pi].[prd_start_dt] AS [start_date]

	FROM [DataWarehouse].[silver].[crm_prd_info] AS [pi]
	LEFT JOIN [DataWarehouse].[silver].[erp_px_cat_g1v2] ct
		ON [pi].[cat_id] = [ct].[id]

	WHERE [pi].[prd_end_dt] IS NULL -- depends on bussiness requirements, but we filter here to remove historical data

	--SELECT * FROM [DataWarehouse].[silver].[erp_px_cat_g1v2]