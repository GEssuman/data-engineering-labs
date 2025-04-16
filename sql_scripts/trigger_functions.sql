
--Logging restock event into inventory_logs table 
CREATE OR REPLACE FUNCTION log_restock() 
	RETURNS TRIGGER AS $$
BEGIN
	INSERT INTO inventory_logs(
	event_type,
	product_id,
	prev_price,
	new_price,
	prev_quantity,
	new_quantity
	)
	VALUES (
		'restock',
		NEW.product_id,
		OLD.price,
		NEW.price,
		OLD.stock,
		NEW.stock
	);
RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--Creating the trigger to execute trigger function, log_restock() 
CREATE OR REPLACE TRIGGER trg_restock
AFTER UPDATE OF stock ON products
FOR EACH ROW
WHEN (OLD.stock < NEW.stock)
EXECUTE FUNCTION log_restock();


--Logging order events into inventory_logs table
CREATE OR REPLACE FUNCTION log_order() 
	RETURNS TRIGGER AS $$
DECLARE
	v_product_price NUMERIC(10, 2);
	v_stock INT;
BEGIN
	SELECT price, stock FROM products WHERE product_id= NEW.product_id INTO v_product_price, v_stock;

	INSERT INTO inventory_logs (
	event_type,
	product_id,
	prev_price,
	new_price,
	prev_quantity,
	new_quantity,
	order_id
	)
	VALUES (
		'order',
		NEW.product_id,
		v_product_price,
		v_product_price,
		v_stock,
		v_stock - NEW.quantity,
		NEW.order_id
	);
RETURN NEW;
END;
$$ LANGUAGE plpgsql;


--Creating the trigger to execute trigger function, log_restock() 
CREATE OR REPLACE TRIGGER trg_order
AFTER INSERT ON order_details
FOR EACH ROW
EXECUTE FUNCTION log_order();


--Logging price changes events into inventory_logs table
CREATE OR REPLACE FUNCTION log_price_change()
	RETURNS TRIGGER AS $$
BEGIN 
	INSERT INTO inventory_logs(
		event_type, 
		product_id, 
		prev_quantity, 
		new_quantity, 
		prev_price, 
		new_price
		) 
		VALUES(
		'price_change',
		NEW.product_id, 
		OLD.stock, 
		NEW.stock, 
		OLD.price, 
		NEW.price
	);
	RETURN NEW; 
END
$$ LANGUAGE plpgsql;


--Creating the trigger to execute trigger function, log_price_change() 
CREATE OR REPLACE TRIGGER trg_price_change
AFTER UPDATE ON products
FOR EACH ROW 
WHEN (NEW.price IS DISTINCT FROM OLD.price)
EXECUTE FUNCTION log_price_change()
 
	