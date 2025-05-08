-- Active: 1746628175650@@localhost@5433@real_time_heartbeat_db
-- ============================
-- PostgreSQL Init Script
-- Database & Schema for Heart Rate Aggregates System
-- ============================

-- Create database (safe to ignore "already exists" error on Docker re-run)
CREATE DATABASE real_time_heartbeat_db;

-- Switch to the new database
\connect real_time_heartbeat_db;

-- ============================
-- Drop tables if they already exist
-- ============================

DROP TABLE IF EXISTS heart_rate_aggregates;
DROP TABLE IF EXISTS users;

-- ============================
-- Users table :- 
-- ============================

-- Although the `users` table is optional for processing raw heart rate data,
-- it provides essential context for analysis, alerting, and reporting.

CREATE TABLE users (
    user_id VARCHAR PRIMARY KEY,
    full_name VARCHAR,
    age INT,
    gender VARCHAR(10)
);


--Inserting some user information based n the number of users being used in the heartbeat simulation

INSERT INTO users (user_id, full_name, age, gender) VALUES
('CUST_0', 'Alice Johnson', 28, 'Female'),
('CUST_1', 'Bob Smith', 35, 'Male'),
('CUST_2', 'Carol Lee', 42, 'Female'),
('CUST_3', 'David Kim', 31, 'Male');

-- ============================
-- Heart rate aggregates (time-series data)
-- ============================

CREATE TABLE heart_rate_aggregates (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    user_id VARCHAR NOT NULL,
    avg_heartbeat FLOAT NOT NULL,
    PRIMARY KEY (window_start, user_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE INDEX idx_agg_user_time
ON heart_rate_aggregates (user_id, window_start DESC);

