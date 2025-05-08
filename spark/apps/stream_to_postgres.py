from pyspark.sql.types import StructField, StructType, StringType, TimestampType, IntegerType
import json
import os
from util import *

schema = StructType([ 
    StructField('sensor_id', StringType(), True), 
    StructField('user_id', StringType(), True), 
    StructField('heartbeat', IntegerType(), True), 
    StructField('timestamp', TimestampType(), True)
]) 

spark = createSparkSession("StreamHeartRate")
KAFKA_BROKER = os.environ.get("KAFKA_BROKER")
TOPIC_NAME = os.environ.get("TOPIC_NAME")


POSTGRES_USER= os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD= os.getenv('POSTGRES_PASSWORD')


output_sink = {
    'url':f"jdbc:postgresql://postgres_db:5432/real_time_heartbeat_db",

    'properties': {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
    }

}

df = subscribe_kafka_stream(spark, "broker:29092", TOPIC_NAME, schema)


agg_df = df.withWatermark("timestamp", "2 minutes").groupBy(
        F.window(F.col("timestamp"), "1 minute", "10 seconds"),
        F.col("user_id")
        ).agg(
            F.avg("heartbeat").alias("avg_heartbeat")
        )

final_df = agg_df.select(
    F.col("window.start").alias("window_start"),
    F.col("window.end").alias("window_end"),
    F.col("user_id"),
    F.col("avg_heartbeat")
)

query = writeStream(final_df, output_sink)


query.awaitTermination()