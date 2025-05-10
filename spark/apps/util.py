from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def createSparkSession(appName):
    spark = SparkSession.builder.appName(appName)\
    .getOrCreate()

    return spark


def subscribe_kafka_stream(spark, kafka_servers, topic, schema):
    df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .load()
    
    df = df.select(F.from_json(F.col("value").cast("string"), schema).alias("data")).select("data.*")

    return df

def subscribe_csv_stream(spark, schema, file_dir):
    df = spark \
    .readStream \
    .option("header", "true") \
    .schema(schema) \
    .csv(file_dir)

    return df


def transform_data(df):
   pass

def write_to_console(df):
    df = df.orderBy(F.col("window_start").desc())
    query = df.writeStream \
    .outputMode('complete') \
    .format("console") \
    .start()

    return query

def write_to_postgres(batch_df, batch_id, output_sink):
    try:
    
        batch_df.write.jdbc(
            url=output_sink.get('url'),
            table="heart_rate_aggregates",
            mode="append",
            properties=output_sink.get('properties')
        ).save()
        print(f"[BATCH {batch_id}] Successfully wrote batch of {batch_df.count()} rows.")
    except Exception as e:
        print(f"[BATCH {batch_id}]: {e}")



def writeStream(df, output_sink):
    query = df.writeStream \
    .foreachBatch(lambda batch_df, batch_id: write_to_postgres(batch_df, batch_id, output_sink)) \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/spark/checkpoints/heartbeat") \
    .trigger(processingTime='30 seconds') \
    .start()

    return query