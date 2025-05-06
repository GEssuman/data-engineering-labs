from pyspark.sql.types import StructField, StructType, StringType, FloatType, IntegerType
import json
import os
from util import *

schema = StructType([ 
    StructField('sensor_id', StringType(), True), 
    StructField('user_id', StringType(), True), 
    StructField('heartbeat', StringType(), True), 
    StructField('timestamp', StringType(), True)
]) 

spark = createSparkSession("StreamHeartRate")
KAFKA_BROKER = os.environ.get("KAFKA_BROKER")
TOPIC_NAME = os.environ.get("TOPIC_NAME")

df = subscribe_kafka_stream(spark, "broker:29092", TOPIC_NAME, schema)

query = write_to_console(df)

query.awaitTermination()