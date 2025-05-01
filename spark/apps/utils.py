from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def createSparkSession(appName):
    spark = SparkSession.builder.appName(appName)\
    .config("spark.jars", "/opt/real-time-spark/spark/resources/postgresql-42.7.2.jar") \
    .config("spark.driver.extraClassPath", "/opt/real-time-spark/spark/resources/postgresql-42.7.2.jar") \
    .config("spark.executor.extraClassPath", "/opt/real-time-spark/spark/resources/postgresql-42.7.2.jar") \
    .getOrCreate()

    return spark

def subscribe_csv_stream(spark, schema, file_dir):
    df = spark \
    .readStream \
    .option("header", "true") \
    .schema(schema) \
    .csv(file_dir)

    return df


def transform_data(df, event_type):
    if event_type == "purchase":
        df = df.withColumn("date_purchased", 
                                                        F.to_timestamp(F.col("date_purchased"), "dd-MM-yyyy, HH:mm:ss"))
        df = df.withColumnRenamed("date_purchased", "event_date")
    else:
        df = df.withColumn("date_viewed", 
                                                        F.to_timestamp(F.col("date_viewed"), "dd-MM-yyyy, HH:mm:ss"))
        df = df.withColumnRenamed("date_viewed", "event_date")
    df = df.withColumn("customer_name", 
                                                        F.concat(F.col("customer_surname"), F.lit(" "), F.col("customer_firstname")))
    df = df.drop('customer_surname', 'customer_firstname')

    return df

def write_to_postgres(batch_df, batch_id, output_sink):
    batch_df.write.jdbc(
        url=output_sink.get('url'),
        table="event_log",
        mode="append",
        properties=output_sink.get('properties')
    )


def writeStream(df, output_sink):
    query = df.writeStream \
    .foreachBatch(lambda batch_df, batch_id: write_to_postgres(batch_df, batch_id, output_sink)) \
    .outputMode("append") \
    .trigger(processingTime="3 seconds") \
    .start()

    return query