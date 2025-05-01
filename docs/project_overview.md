
# Project Overview

## Real-Time Data Ingestion Using Spark Structured Streaming & PostgreSQL

This project presents a real-time data pipeline that simulates an e-commerce platform's activity tracking system. The pipeline encompasses generating synthetic user events, processing real-time data streams through Apache Spark Structured Streaming, and storing processed information in a PostgreSQL database.

### Project Objective
- Simulate and Ingest Streaming Data
- Use Spark Structured Streaming To Process Data in Real Time
- Store and Verify Processed Data in A PostgreSQL Database
- Understand the Architecture of A Real-time Data Pipeline
- Measure and Evaluate System Performance

### Tools & Technologies 
- Apache Spark Structured Streaming
- PostgreSQL
- Python (for data generation)
- SQL (for database setup)


## Project Folder Structure
```
real-time-data-ingestion/
│
├── docker-compose.yml         # Your docker-compose file (containers: postgres, spark-master, spark-worker)
│
├── spark/
│   ├── apps/
│   └─── resources/
│       └── postgresql-42.7.2.jar # PostgreSQL JDBC driver
│
├── e-commerce-user-events/             # The folder where synthetic user events are saved
│   |── product-purchase-events/        # 
|   └── product-view-events/
│
|
├──scripts/
|   ├── python/                    # script generating the dummy ecommerce product evnnts by users
|   └─── sql/                     # sql script to create database and table
├── docs/
|   └─── sql/                      # Setup instructions
└── .env                         # Environment variables like POSTGRES_PASSWORD
```




### Project Architecture

- **Data Ingestion Simulation**  
  The system uses the **Factory Design Pattern** to modularly and extensibly manage event generators. Each event generator creates randomized, dummy data that is exported to a **new, uniquely named CSV file** on each execution.

  #### Event Generation and Export
    On each run:
    - The system generates 25–50 random events of each type.
        - Creates a new `.csv` file in the appropriate subdirectory:
        - `product-view-events/`
        - `product-purchase-events/`
    - File names are suffixed with a UUID, e.g.:
        - `product_view_events_18e07c3a.csv`
        - `product_purchase_event_a8b83b92.csv`



- **Streaming Data Using Spark Structured Streaming**  
  Spark monitors the directories where CSVs are generated and processes new files, transforming the data and writing results into PostgreSQL.

- **Load Data in PostgreSQL Database**  
  Transformed streaming data is loaded into a PostgreSQL event log table using Spark's JDBC connector.
