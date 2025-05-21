CREATE DATABASE bangladesh_flight_final_db;


CREATE TABLE bangladesh_flight_analytics (
    airline VARCHAR(100),
    source VARCHAR(100),
    source_name VARCHAR(255),
    destination VARCHAR(10),
    destination_name VARCHAR(255),
    departure_datetime TIMESTAMP,
    arrival_datetime TIMESTAMP,
    duration DOUBLE PRECISION,
    stopovers VARCHAR(10),
    aircraft_type VARCHAR(255),
    class VARCHAR(20),
    booking_source VARCHAR(100),
    base_fare NUMERIC(10, 2),
    tax_surcharge NUMERIC(10, 2),
    total_fare NUMERIC(10, 2),
    seasonality VARCHAR(20),
    days_before_departure INT,
    peak_season VARCHAR(50),
    PRIMARY KEY (airline, source, destination, departure_datetime)
);


CREATE TABLE avg_fare_per_airline (
    airline  VARCHAR(100),
    avg_base_fare NUMERIC(10,2),
    avg_total_fare NUMERIC(10,2),
    booking_count INT,
    PRIMARY KEY (airline)
)



CREATE TABLE peak_season_summary (
    peak_season  VARCHAR(100),
    avg_base_fare NUMERIC(10,2),
    avg_total_fare NUMERIC(10,2),
    booking_count INT,
    PRIMARY KEY (peak_season)
)


CREATE TABLE top_source_dest_pairs (
    source  VARCHAR(50),
    destination VARCHAR(50),
    booking_count INT,
    PRIMARY KEY (source, destination)
)