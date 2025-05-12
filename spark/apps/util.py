from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import psycopg2

import logging
import datetime


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


alert_logger = logging.getLogger('heartbeat_alerts')
alert_logger.setLevel(logging.INFO)
alert_handler = logging.FileHandler(f'heartbeat_alerts_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
alert_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
alert_logger.addHandler(alert_handler)



def createSparkSession(appName):
    try:
        spark = SparkSession.builder.appName(appName)\
        .getOrCreate()
        logging.info(f"Spark Session succesfully created:\n {spark}")
        return spark
    except Exception as e:
        logging.error(f"Unable to create Spark Session :\n {e}")

    


def subscribe_kafka_stream(spark, kafka_servers, topic, schema):
    try: 
        df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_servers) \
                .option("subscribe", topic) \
                .option("startingOffsets", "latest") \
                .load()
    
        df = extract_value_kafa_msg(df, schema=schema)

        return df
    except Exception as e:
        logging.error(f"Unable to Subscribe to the Broker Server: {kafka_servers}\nError :- {e}")


def extract_value_kafa_msg(df, schema):
    try:
        df = df.select(F.from_json(F.col("value").cast("string"), schema).alias("data")).select("data.*")
        return df
    except Exception as e:
        logging.error(f"Error:- {e}")


def transform_data(df):
    try: 
        agg_df = df.withWatermark("timestamp", "5 seconds").groupBy(
            F.window(F.col("timestamp"), "15 seconds"),
                F.col("user_id")
                    ).agg(
                        F.avg("heartbeat").cast("int").alias("avg_heartbeat")
                    )
        
        final_df = agg_df.select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("user_id"),
            F.col("avg_heartbeat")
        )
   
        return final_df
    except Exception as e:
        logging.error(f"Error-: {e}")


def write_to_console(df):
    try: 
        df = df.orderBy(F.col("window_start").desc())
        query = df.writeStream \
        .outputMode('complete') \
        .format("console") \
        .start()

        return query
    except Exception as e:
        logging.error(f"Could not write to console-: {e}")

def write_to_postgres(batch_df, batch_id, output_sink):
    try:

        abnormal_rows = batch_df.filter((F.col("avg_heartbeat") > 110) | (F.col("avg_heartbeat") < 60)).collect()
        for row in abnormal_rows:
            alert_logger.info(
                f"[ALERT] User: {row.user_id} | Avg Heartbeat: {row.avg_heartbeat} | "
                f"Window: {row.window_start} - {row.window_end}"
            )

        batch_df.write \
        .format("jdbc") \
        .option("url", output_sink.get('url')) \
        .option("driver", output_sink.get('driver')) \
        .option("dbtable", "heart_rate_aggregates") \
        .option("user", output_sink.get('user')) \
        .option("password", output_sink.get('password')) \
        .mode("append") \
        .save()

        logging.info(f"[BATCH {batch_id}] Successfully wrote batch of {batch_df.count()} rows.")

    except Exception as e:
        logging.error(f"[BATCH {batch_id}]: {e}")

def write_to_postgres_upsert(batch_df, batch_id, output_sink):
    try:
        conn = psycopg2.connect(
            host="postgres_db",
            database="real_time_heartbeat_db",
            user=output_sink.get('user'),
            password=output_sink.get('password')
        )
        cur = conn.cursor()
        
        for row in batch_df.collect():
            cur.execute("""
            INSERT INTO heart_rate_aggregates (window_start, window_end, user_id, avg_heartbeat)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (window_start, user_id) DO UPDATE SET
            avg_heartbeat = EXCLUDED.avg_heartbeat
        """, (row.window_start, row.window_end, row.user_id, row.avg_heartbeat))

        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"[BATCH {batch_id}] Successfully wrote batch of {batch_df.count()} rows.")

    except Exception as e:
        logging.error(f"[BATCH {batch_id}]: {e}")



def writeStream(df, output_sink):
    try:
        query = df.writeStream \
        .foreachBatch(lambda batch_df, batch_id: write_to_postgres(batch_df, batch_id, output_sink)) \
        .outputMode("append") \
        .option("checkpointLocation", "/opt/spark/checkpoints/heartbeat_11") \
        .trigger(processingTime='10 seconds') \
        .start()   
        return query
    except Exception as e:
        logging.error(f"Error:- {e}")
