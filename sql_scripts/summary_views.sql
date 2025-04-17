-- View 1: Orders Per Customer
-- This view summarizes customer orders by including number of items, total amount, and date of the order.
CREATE OR REPLACE VIEW vw_orders_per_customer AS 
WITH order_detail AS (
	SELECT
		order_id,
		SUM(quantity) AS number_of_items  -- Total number of items per order
	FROM order_details 
	GROUP BY order_id
) 
SELECT 
	customer.name AS customer_name,               
	orders.total_amount,                     
	order_detail.number_of_items
	orders.order_date            
FROM orders 
JOIN customers AS customer ON orders.customer_id = customer.customer_id 
JOIN order_detail ON orders.order_id = order_detail.order_id;


-- View 2: Low on Stock
-- Identifies products that are below their reorder level
CREATE OR REPLACE VIEW vw_low_on_stock AS 
SELECT 
	name AS product_name,          
	stock,        
	reorder_level, 
	price          
FROM products
WHERE stock < reorder_level   
ORDER BY stock ASC;           


-- SELECT * FROM low_on_stock;



-- Materialied View 3: Customer Categorization
-- Categorizes customers into Bronze, Silver, and Gold tiers based on their total spending.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_customers_categorization AS
--Get orders made by acustomer and sum of total amount spent 
WITH customer_spending AS (
	SELECT 
		customer_id,
		COUNT(order_id) AS order_count,     
		SUM(total_amount) AS total_amount_spent    
	FROM orders
	GROUP BY customer_id
)
-- Inner Join the customer table with customer_spending table created as CTE
SELECT 
	cs.customer_id,
	c.name AS customer_name,                    
	cs.total_amount_spent,           
	cs.order_count,  
	CASE
		WHEN cs.total_amount_spent < 500 THEN 'Bronze'                -- Low spenders
		WHEN cs.total_amount_spent >= 500 AND cs.total_amount_spent < 1500 THEN 'Silver' -- Medium spenders
		ELSE 'Gold'                                            -- High spenders
	END AS category            -- column to categorize customers per the total amount spent on orders 
FROM customer_spending cs 
JOIN customers c ON cs.customer_id = c.customer_id;


-- View the categorized customer data
-- SELECT * FROM customers_categorization;



-- View 4: Orders_info
-- A view that summarizes order information
CREATE VIEW vw_orders_info AS
    WITH order_detail_info AS (
        SELECT 
            order_id,
            SUM(quantity) AS number_of_items  -- Total number of items in each order
        FROM order_details
        GROUP BY order_id
    )
    SELECT 
        c.name,
        o.total_amount,
        od.number_of_items,
        o.order_date
    FROM orders AS o
    JOIN order_detail_info AS od ON o.order_id = od.order_id
	JOIN customers c ON o.customer_id = c.customer_id
    ORDER BY o.order_date DESC;  -- Most recent orders appear first
