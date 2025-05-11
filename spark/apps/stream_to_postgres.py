from pyspark.sql.types import StructField, StructType, StringType, TimestampType, IntegerType
import os
from util import *


KAFKA_BROKER = os.environ.get("KAFKA_BROKER")
TOPIC_NAME = os.environ.get("TOPIC_NAME")


POSTGRES_USER= os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD= os.getenv('POSTGRES_PASSWORD')


schema = StructType([ 
    StructField('sensor_id', StringType(), True), 
    StructField('user_id', StringType(), True), 
    StructField('heartbeat', IntegerType(), True), 
    StructField('timestamp', TimestampType(), True)
]) 

output_sink = {
    'url':f"jdbc:postgresql://postgres_db:5432/real_time_heartbeat_db",
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}


spark = createSparkSession("StreamHeartRate")
df = subscribe_kafka_stream(spark, "broker:29092", TOPIC_NAME, schema)

final_df = transform_data(df)

# query = write_to_console(final_df)


query = writeStream(final_df, output_sink)
query.awaitTermination()

