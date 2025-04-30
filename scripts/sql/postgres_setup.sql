-- Active: 1746024913054@@localhost@5433@realtime_event_db@public

CREATE DATABASE RealTime_Event_DB


CREATE TABLE event_log(
    event_id SERIAL PRIMARY KEY,
    event_type VARCHAR(20) CHECK (event_type IN ('product_view', 'product_purchase')),
    product_name VARCHAR(200) NOT NULL,
    customer_name VARCHAR(200) NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    quantity INT,
    event_date TIMESTAMP 
)