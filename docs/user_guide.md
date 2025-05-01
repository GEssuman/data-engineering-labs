
# User Guide

## Requirements
- Docker
- Docker Compose

## Project Setup
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


## Running Event Generator

```bash
cd scripts/python
python data_generator.py
```

## Running Spark Structured Streaming

1. Access Spark master container
```bash
docker exec -it real-time-spark-master bash
cd /opt/real-time-spark/spark/apps
```

2. Run Spark application
```bash
spark-submit --jars /opt/real-time-spark/spark/resources/postgresql-42.7.2.jar spark_streaming_to_postgres.py
```
