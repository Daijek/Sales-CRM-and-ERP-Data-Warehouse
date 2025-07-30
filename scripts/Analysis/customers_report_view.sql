/*
-- =====================================================================================
-- Change Over Time Analysis
-- =====================================================================================
-- This gets the total sales for each day
SELECT 
	order_date,
	SUM(sales_amount) total_revenue

FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY order_date
ORDER BY order_date

-- This gets the total sales for each year
SELECT 
	YEAR(order_date) AS order_date,
	SUM(sales_amount) total_revenue,
	COUNT(DISTINCT customer_key),
	SUM(quantity) as total_quantity

FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY YEAR(order_date)

-- This gets the total sales for each month in a year
SELECT 
	YEAR(order_date) AS [year],
	MONTH(order_date) AS [month],
	SUM(sales_amount) total_revenue,
	COUNT(DISTINCT customer_key) total_customers,
	SUM(quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date), SUM(sales_amount)

-- =====================================================================================
-- Cummulative Analysis
-- =====================================================================================

-- This gets the total sales per month
SELECT 
	YEAR(order_date) AS [year],
	MONTH(order_date) AS [month],
	SUM(sales_amount) total_revenue,
	COUNT(DISTINCT customer_key) total_customers,
	SUM(quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date), SUM(sales_amount)

-- This gets the running total of sales over time
SELECT 
	[year],
	[month],
	total_revenue,
	SUM(total_revenue) OVER(PARTITION BY [year] ORDER BY [year],[month]) cumulative_total_revenue,
	AVG(total_revenue) OVER(PARTITION BY [year] ORDER BY [year],[month]) total_revenue_moving_average
	
FROM (
	SELECT
		YEAR(order_date) AS [year],
		MONTH(order_date) AS [month],
		SUM(sales_amount) total_revenue,
		COUNT(DISTINCT customer_key) total_customers,
		SUM(quantity) as total_quantity
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY YEAR(order_date), MONTH(order_date)
	)t

-- =====================================================================================
-- Performance Analysis
-- =====================================================================================
-- Analyzing the yearly performance of products by comparing their sales to both the 
-- average sales performance of the product and the previous years sales
WITH yearly_product_sale AS (
	SELECT 
		YEAR(order_date) order_year,
		p.[name] product_name,
		SUM(f.sales_amount) current_sales
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_products p
		ON f.product_key = p.product_key
	WHERE f.sales_amount IS NOT NULL
	GROUP BY YEAR(order_date), p.[name]
	
)
SELECT 
	order_year,
	product_name,
	current_sales,
	AVG(current_sales) OVER(PARTITION BY product_name) Average_sales,
	current_sales - AVG(current_sales) OVER(PARTITION BY product_name) average_difference,
	CASE 
		WHEN current_sales - AVG(current_sales) OVER(PARTITION BY product_name) > 0 THEN 'Above Average'
		WHEN current_sales - AVG(current_sales) OVER(PARTITION BY product_name) < 0 THEN 'Below Average'
		ELSE 'Average'
	END avg_change,
	--Year-over-year Analysis
	LEAD(current_sales) OVER(PARTITION BY product_name ORDER BY order_year DESC) previous_sales,
	LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year ASC) previous_sales,
	CASE 
		WHEN LEAD(current_sales) OVER(PARTITION BY product_name ORDER BY order_year DESC)  > current_sales THEN 'Decrease'
		WHEN LEAD(current_sales) OVER(PARTITION BY product_name ORDER BY order_year DESC) < current_sales THEN 'Increase' 
		ELSE 'No Change'
	END previous_year_change
FROM yearly_product_sale
ORDER BY product_name, order_year

-- =====================================================================================
-- Part to whole Analysis
-- =====================================================================================

-- Which categories contribute the most to overall sales
SELECT 
	p.category,
	SUM(sales_amount) total_sales,
	FORMAT(
		(SUM(sales_amount) * 100.0)/
		(SELECT SUM(sales_amount) FROM gold.fact_sales), 'N2'
		) + ' %'
FROM gold.fact_sales f
JOIN gold.dim_products p
	ON p.product_key = f.product_key
GROUP BY p.category



-- =====================================================================================
-- Data segmentation
-- =====================================================================================
/*
	Segment products into cost ranges and count how many products fall into each segment
*/
WITH segmented_cost AS (
	SELECT
		p.[name] product_name,
		CASE
			WHEN f.price >= 0 AND f.price <1000 THEN '0 - 999'
			WHEN f.price >= 1000 AND f.price <2000 THEN '1000 - 1999'
			WHEN f.price >= 2000 AND f.price <3000 THEN '2000 - 2999'
			WHEN f.price >= 3000 AND f.price <4000 THEN '3000 - 3999'
			ELSE '4000 and over'
		END cost_range
	FROM gold.dim_products p 
	LEFT JOIN gold.fact_sales f ON p.product_key = f.product_key 
)
SELECT 
	cost_range,
	COUNT(product_name) number_of_products
FROM segmented_cost
GROUP BY cost_range

/*
	Group customers into three segments based on their spending behavior:
	- VIP: Customers with at least 12 months of history and spending more than $5,000.
	- Regular: Customers with at least 12 months of history but spending $5,000 or less.
	- New: Customers with a life span less than 12 months.

	Find the total number of customers in each group
*/

SELECT 
	COUNT(customer_key) total_customers,
	spending_behaviour

FROM (
	SELECT 
		customer_key,
		CASE
			WHEN customer_life_span >= 12 AND total_spent > 5000 THEN 'VIP'
			WHEN customer_life_span >= 12 AND total_spent <= 5000 THEN 'Regular'
			ELSE 'New'
		END spending_behaviour
	FROM(
		SELECT 
			c.customer_key,
			SUM(f.sales_amount) total_spent,
			DATEDIFF(month, MIN(f.order_date), MAX(f.order_date)) AS customer_life_span
		FROM gold.dim_customers c
		JOIN gold.fact_sales f ON f.customer_key = c.customer_id
		GROUP BY c.customer_key
	)t
)u
GROUP BY spending_behaviour */

-- =====================================================================================
-- Customer Report
-- =====================================================================================
/*
	Purpose:
		- This report consolidates key customer metrics and behaviors
	Highlights:
		1. Gather essential fields such as names, ages, and transaction details.
		2. segment customers into categories (VIP, Regular, New) and age groupd.
		3. Aggregates customer-level metrics:
			- total orders
			-total sales
			- total quantity purchased
			- total products
			- lifespan (in months)
		4. calculate caluables KPIs:
			- recency (months since last order)
			- average order value
			- average monthly spend
-- =====================================================================================
*/

CREATE VIEW gold.report_customers AS 
	WITH base_query AS (
	-- ---------------------------------------------------------------------------------------
	-- 1) Base Query: Retrieves core columns from tables
	------------------------------------------------------------------------------------------
		SELECT 
			f.order_number,
			f.product_key,
			f.order_date,
			f.sales_amount,
			f.quantity,
			f.customer_key,
			c.customer_number,
			c.first_name + ' ' +c.last_name AS customer_name,
			DATEDIFF(year, c.birthdate, GETDATE()) age
	
		FROM gold.fact_sales f
		LEFT JOIN gold.dim_customers c
			ON c.customer_key = f.customer_key
		WHERE order_date IS NOT NULL

	), customer_aggregation AS (
	-- ---------------------------------------------------------------------------------------
	-- 2) Customer Aggregations: Summarizes key metrics at the customer level
	------------------------------------------------------------------------------------------
		SELECT
			customer_key,
			customer_number,
			customer_name,
			age,
			COUNT(DISTINCT order_number) AS total_orders,
			SUM(sales_amount) AS total_sales,
			SUM(quantity) AS total_quantity,
			COUNT(DISTINCT product_key) AS total_products,
			MAX(order_date) AS last_order_date,
			DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
		FROM base_query
		GROUP BY customer_key, customer_number, customer_name, age
	)

	SELECT
		customer_key,
		customer_number,
		customer_name,
		age,
		CASE
			WHEN age < 20 THEN 'Under 20'
			WHEN age between 20 and 29 THEN '20-29'
			WHEN age between 30 and 39 THEN '30-39'
			WHEN age between 40 and 49 THEN '40-49'
			ELSE '50 and above'
		END age_group,
		CASE
			WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
			WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
			ELSE 'New'
		END customer_segment,
		total_orders,
		total_sales,
		total_quantity,
		total_products,
		last_order_date,
		DATEDIFF(month, last_order_date, GETDATE()) recency,
		lifespan,
		-- Compute average order value 
		CASE 
			WHEN total_orders = 0 THEN 0
			ELSE total_sales / total_orders
		END AS avg_order_value,

		-- Comput average monthly spend
		CASE 
			WHEN lifespan = 0 THEN total_sales
			ELSE total_sales / lifespan
		END avg_aggregation
	FROM customer_aggregation