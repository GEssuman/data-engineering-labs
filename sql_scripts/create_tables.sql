-- Create Customers table
CREATE TABLE IF NOT EXISTS customers(
	customer_id SERIAL PRIMARY KEY,
	name VARCHAR(200) UNIQUE,
	email VARCHAR(200),
	phone_number VARCHAR(15)
);

-- Index on customer_id to speed up joins and lookups involving customers,
-- particularly when joining with the `orders` table or filtering customers.
CREATE INDEX idx_customer_customer_id ON customers(customer_id);

-- Create Products Table
CREATE TABLE IF NOT EXISTS products(
	product_id SERIAL PRIMARY KEY,
	name VARCHAR(255) NOT NULL,
	category VARCHAR(200),
    price NUMERIC(10,2) NOT NULL CHECK (price > 0),
	stock INT NOT NULL CHECK (stock >= 0),
	reorder_level INT NOT NULL DEFAULT 10
);


-- Composite index on stock and reorder_level improves performance for
-- queries checking low stock levels (e.g., stock < reorder_level),
-- such as in inventory monitoring or restock alerts.
CREATE INDEX idx_products_stock_reorder ON products(stock, reorder_level);

-- Index on product_id to support efficient joins and lookups involving products,
-- especially useful for queries that reference products in the `order_details` table.
CREATE INDEX idx_products_product_id ON products(product_id);


-- Create Orders Table
CREATE TABLE IF NOT EXISTS orders(
	order_id SERIAL PRIMARY KEY,
	customer_id INT NOT NULL,
	order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	total_amount NUMERIC(10, 2) NOT NULL CHECK (total_amount >= 0),
	CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);



-- Index on customer_id in the orders table to enhance lookup and join speed
-- when aggregating orders by customer, such as in customer activity or reporting views.
CREATE INDEX idx_orders_customer_id ON orders(customer_id);



--Create Order Details Table
CREATE TABLE IF NOT EXISTS order_details(
	order_details_id SERIAL PRIMARY KEY,
	order_id INT NOT NULL,
	product_id INT NOT NULL, 
	quantity INT NOT NULL CHECK (quantity > 0),
	unit_price NUMERIC(10, 2) NOT NULL,
	CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES orders(order_id),
	CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(product_id)
);

--Add discount column to order_details
ALTER TABLE order_details 
ADD COLUMN discount NUMERIC(4,2) DEFAULT 0;


-- Index on order_id in the order_details table for faster joins with the
-- `orders` table
CREATE INDEX idx_order_details_order_id ON order_details(order_id);


-- Create iNventory logs table 
CREATE TABLE IF NOT EXISTS inventory_logs(
	log_id SERIAL PRIMARY KEY,
	event_type VARCHAR(50) CHECK ( event_type IN (
        'order', 
        'restock', 
        'price_change', 
        'product_registration'
        )
    ),
	product_id INT NOT NULL REFERENCES products(product_id),
	order_id INT REFERENCES orders(order_id),
	prev_price NUMERIC(10, 2) NOT NULL,
	new_price NUMERIC(10, 2) NOT NULL,
	prev_quantity NUMERIC(10, 2) NOT NULL,
	new_quantity NUMERIC(10, 2) NOT NULL,
	event_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index on event_type in inventory_logs to improve performance when filtering
-- logs by event type (e.g., 'order', 'restock', etc.) during auditing.
CREATE INDEX idx_inventory_logs_event_type ON inventory_logs(event_type);
