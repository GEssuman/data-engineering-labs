
# Real-Time Data Ingestion Using Spark Structured Streaming & PostgreSQL

This project presents a real-time data pipeline that simulates an e-commerce platform's activity tracking system. 
The pipeline encompasses generating synthetic user events, processing real-time data streams through Apache Spark Structured 
Streaming, and storing processed information in a PostgreSQL database.


## Project Objective
- Simulate and Ingest Streaming Data
- Use Spark Structured Streaming To Process Data in Real Time
- Store and Verify Processed Data in A PostgreSQL Database
- Understand the Architecture of A Real-time Data Pipeline
- Measure and Evaluate System Performance


## Tools & Technologies 
 - Apache Spark Structured Streaming 
 - PostgreSQL 
 - Python (for data generation) 
 - SQL (for database setup) 

## Project Structure
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



## Project Setup Using Docker

### Requirements:
- Docker
- Docker Compose

### Setup Instructions:

1. **Clone the repository**  
   First, clone the repository to your local machine:
   ```bash
   git clone https://github.com/GEssuman/data-engineering-labs.git
   cd data-engineering-labs

2. **Switch to the correct branch**
    ```
    git checkout spark/real-time-data-ingestion
    ```
3. **Run Docker Compose**
    Start the services defined in the docker-compose.yml:
    ```
    docker-compose up -d
    ```

4. **Verify that the services are running**
    After running the command, the services (PostgreSQL and Spark) will be up and running in your Docker environment. You can check the status of the containers by running:
    ```
    docker ps
    ```

    ## Services Running:
    - PostgreSQL Database: A PostgreSQL container running with the postgres_db service.
    - Spark Master: The Spark master node available at port 7077.
    - Spark Worker: A Spark worker node for processing tasks.

## Project Architecture
- **Data Ingestion Simulation**
- **Streaming Data Using Spark Stuctured Streaming**
- **Load Data in PostgreSQL Database**


### Data Ingesetion Streaming
The system uses the **Factory Design Pattern** to modularly and extensibly manage event generators. Each event generator creates randomized, dummy data that is exported to a **new, uniquely named CSV file** on each execution—making it compatible with **Spark Structured Streaming**, which requires new files for each batch of streamed data.

#### Event Generation and Export

On each run:
- The system generates 25–50 random events of each type.
- Creates a new `.csv` file in the appropriate subdirectory:
  - `product-view-events/`
  - `product-purchase-events/`
- File names are suffixed with a UUID, e.g.:
  - `product_view_events_18e07c3a.csv`
  - `product_purchase_event_a8b83b92.csv`

> This design ensures **Spark Structured Streaming** can detect and process new files reliably.