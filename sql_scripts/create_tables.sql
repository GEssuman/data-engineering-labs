-- Create Customers table
CREATE TABLE IF NOT EXISTS customers(
	customer_id SERIAL PRIMARY KEY,
	name VARCHAR(200) UNIQUE,
	email VARCHAR(200),
	phone_number VARCHAR(15)
);


-- Create Products Table
CREATE TABLE IF NOT EXISTS products(
	product_id SERIAL PRIMARY KEY,
	name VARCHAR(255) NOT NULL,
	category VARCHAR(200),
    price NUMERIC(10,2) NOT NULL CHECK (price > 0),
	stock INT NOT NULL CHECK (stock >= 0),
	reorder_level INT NOT NULL DEFAULT 10
);


-- Create Orders Table
CREATE TABLE IF NOT EXISTS orders(
	order_id SERIAL PRIMARY KEY,
	customer_id INT NOT NULL,
	order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	total_amount NUMERIC(10, 2) NOT NULL CHECK (total_amount >= 0),
	CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);




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

