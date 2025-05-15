
# Real-Time Heart Rate Monitoring System

Developing an End-to-End Data Pipeline for Flight Price Analysis in Bangladesh

## **Project Objective**
To build a reliable and automated data pipeline using Apache Airflow that processes flight price data for Bangladesh from ingestion to analysis. The pipeline is designed to:

- Ingest raw flight pricing CSV data.

- Validate and Stage the data to MySQL.

- Transform the data into a suitable structure.

- Compute key performance indicators (KPIs).

- Store the final output in a PostgreSQL database for downstream analysis and visualization.





## **Tools & Technologies Used**
- Apache Airflow: Orchestration of ETL pipeline.
- Pandas: Data transformation and KPI computation.
- PostgreSQL: Final storage of cleaned and aggregated data.
- Docker: Environment management.
- MySQL (intermediate or initial staging).
- Python: Custom ETL logic and database interactions.
- Logging: For monitoring pipeline success and errors.


## **User Guide**

1. **Clone the repository**  
```bash
git clone https://github.com/GEssuman/data-engineering-labs.git
cd data-engineering-labs
```

2. **Switch to the correct branch** 
```bash
git checkout airflow/flight-price-analysis
```


3. **Environment Setup**
Create `.env` File

   To configure environment variables for your services, create a `.env` file in your project directory and populate it with the appropriate values from the `.env.example` file. This will configure your local environment for connecting to the various services like MySql,and PostgreSQL.
---


4. **Start all services**  
```bash
docker compose up --build
```

## **Docker Compose Services**

- **MYSQL**: `mysql:latest`
- **POSTGRESQL**: `postgres:14`
---


## *Pipeline Overview**
1. Data Ingestion
Raw CSV files containing flight data are extracted form kaggle.

Files are parsed using pandas and read into memory preparing for staging.


2. Data Validation & Satagin
Columns are renamed and standardized to match database schema requirements

Datatypes such as datetime, float, and int are validated and converted appropriately.

Data is staged into a MySQL database.
---

3. KPI Computation
***---***

4. Database Storage
Final cleaned and enriched dataset is written to a PostgreSQL database.

Schema includes fields like airline, source, destination, fare, departure time, etc.