--Create a procedure to register new products
CREATE OR REPLACE PROCEDURE proc_register_product(
	p_name VARCHAR(255), 
	p_category VARCHAR(200),
	p_price NUMERIC(10, 2),
	p_stock INT,
	p_reorder_level INT
) AS $$
BEGIN 
	INSERT INTO products(
		name, category,price, stock, reorder_level
	) VALUES (
	p_name, p_category, p_price, p_stock, p_reorder_level
	);
END;
$$ LANGUAGE plpgsql;

--Example 
--CALL proc_register_product('Pear', 'Fruits', 5.00, 27, 5);


--Create a procedure to add new customer to the customer table
CREATE OR REPLACE PROCEDURE proc_register_customer(
	p_name VARCHAR(200), 
	p_email VARCHAR(200),
	p_phone_number VARCHAR(15)
) AS $$
BEGIN 
	INSERT INTO customers(
		name, email, phone_number
	) VALUES (
	p_name, p_email, p_phone_number
	);
END;
$$ LANGUAGE plpgsql;
--Example:
--CALL register_customer('Essuman Godsaves', 'essuman@gmail.com', '+233245157826');


---Create a procedure to restock products(i.e: increase the stocke of a given product)
CREATE OR REPLACE PROCEDURE proc_restock_product(
	p_product_id INT,
	quantity INT
) AS $$
BEGIN
	UPDATE products
	SET stock = stock + quantity 
    WHERE product_id = p_product_id;
END;
$$ LANGUAGE plpgsql;

-- Example;
--CALL restock_product(1, 20);


--Create a procedure to change the price of a specific product
CREATE OR REPLACE PROCEDURE proc_change_product_price(
	p_product_id INT,
	p_new_price NUMERIC(10, 2)
) AS $$
BEGIN
	UPDATE products
	SET price = p_new_price 
    WHERE product_id = p_product_id;
END;
$$ LANGUAGE plpgsql;
-- Example;
--CALL change_product_price(1, 35.00);



--Create a procedure to process order placement by a customer
CREATE OR REPLACE PROCEDURE proc_make_order(
	p_customer_id INT,
	p_product_ids INT[],
	p_quantities INT[]
) AS $$
DECLARE
	v_order_id INT;
	v_total_amount NUMERIC(10, 2) := 0;
	v_price NUMERIC(10, 2);
	v_discount_rate NUMERIC(10, 2);
	v_final_price NUMERIC(10, 2);
BEGIN
	
    --check array lengths if they are both equal
	IF array_length(p_product_ids, 1) <> array_length(p_quantities, 1) THEN
   		RAISE EXCEPTION 'Product IDs and Quantities arrays must be of same length';
	END IF;

    --Insert new order with initial amount 0 and return it order_id into the v_order_id varaible
	INSERT INTO orders(customer_id, total_amount)
		VALUES (p_customer_id, v_total_amount) 
        RETURNING order_id INTO v_order_id;

    -- Loop through each product in the order
	FOR i IN 1..array_length(p_product_ids, 1) LOOP
		
		SELECT price INTO v_price FROM products WHERE product_id=p_product_ids[i];
		
        -- Determing the discount based on the qunatity of product
		IF p_quantities[i] >= 20 THEN
			v_discount_rate := 0.15;
        ELSIF p_quantities[i] >= 10 THEN
            v_discount_rate := 0.10;
        ELSIF p_quantities[i] >= 5 THEN
            v_discount_rate := 0.05;
        ELSE
            v_discount_rate := 0.0;
        END IF;

        -- calculate the final price and update the total
		v_final_price := v_price * (1 - v_discount_rate);
		v_total_amount =v_total_amount + (p_quantities[i] * v_final_price);		
	
        -- Insert into the order_details table
		INSERT INTO order_details
			(order_id, product_id, quantity, unit_price) 
			VALUES (v_order_id,  p_product_ids[i], p_quantities[i], v_price);
		
        --Update each product stock
		UPDATE products
		SET stock = stock - p_quantities[i]
		WHERE product_id = p_product_ids[i];

	END LOOP;
	
    -- update the order with the final total amount
	UPDATE orders
	SET total_amount = v_total_amount WHERE order_id = v_order_id;
END;
$$ LANGUAGE plpgsql;

-- Example:
--  CALL proc_make_order(1, ARRAY[1,2], ARRAY[5,10]);