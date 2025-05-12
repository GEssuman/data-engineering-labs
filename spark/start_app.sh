#!/bin/bash

# Start Spark Master normally
# ${SPARK_HOME}/sbin/start-master.sh

# # Small wait to make sure master starts
# sleep 5

# Submit PySpark streaming app (adjust app file name)
spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/resources/spark-sql-kafka-0-10_2.12-3.5.5.jar,\
/opt/spark/resources/spark-token-provider-kafka-0-10_2.12-3.5.5.jar,\
/opt/spark/resources/kafka-clients-3.5.1.jar,\
/opt/spark/resources/commons-pool2-2.11.1.jar,\
/opt/spark/resources/postgresql-42.7.2.jar \
  /opt/spark/apps/stream_to_postgres.py
