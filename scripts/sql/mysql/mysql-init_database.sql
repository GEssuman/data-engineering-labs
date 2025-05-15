CREATE DATABASE bangladesh_flight_db;

USE bangladesh_flight_db;

CREATE TABLE bangladesh_flight (
    flight_id int NOT NULL AUTO_INCREMENT,
    airline  VARCHAR(100),
    source  VARCHAR(100),
    source_name VARCHAR(255),
    destination  VARCHAR(10),
    destination_name VARCHAR(255),
    departure_datetime DATETIME,
    arrival_datetime DATETIME,
    duration DOUBLE(16, 10),
    stopovers VARCHAR(10),
    aircraft_type VARCHAR(255),
    class VARCHAR(20),
    booking_source VARCHAR(100),
    base_fare DOUBLE(10,2),
    tax_surcharge DOUBLE(10,2),
    total_fare DOUBLE(10,2),
    seasonality VARCHAR(20),
    days_before_departure INT,
    PRIMARY KEY (airline, source, destination, departure)
)