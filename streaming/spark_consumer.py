from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from utils import KafkaTopics, logger, eng_stopwords, remove_stopwords
from kafka_manager import broker_url

processed_data_schema = StructType([
    StructField("source", StringType(), True),
    StructField("text", StringType(), True),
    StructField("title", StringType(), True)
])

def start_consumer():
    """Start the Spark consumer."""
    builder = SparkSession.builder.appName("DEAS-Stream-Pipeline").master("local[*]").config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0")
    spark = builder.getOrCreate()

    sc = SparkContext.getOrCreate()
    stopwords_bcst = sc.broadcast(eng_stopwords)

    logger.info("Spark Session created.")

    data_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", broker_url) \
        .option("failOnDataLoss", "false") \
        .option("subscribe", KafkaTopics.SCRAPED_DATA.value).load()
    
    parsed_df = data_df.select(
        F.from_json(F.col("value").cast("string"), processed_data_schema).alias("data")
    ).select("data.*")
    
    processed_df = parsed_df.withColumn("stopwords", F.lit(stopwords_bcst.value)
    ).withColumn("title", remove_stopwords(F.regexp_replace(F.lower(F.col("title")), F.lit(r'[^a-zA-Z\s]'), 
        F.lit("")), F.col("stopwords"))
    ).withColumn("text", remove_stopwords(F.regexp_replace(F.lower(F.col("text")), 
        F.lit(r'[^a-zA-Z\s]'), F.lit("")), F.col("stopwords")))
    
    kafka_output_df = processed_df.select(
        F.to_json(F.struct("source", "text", "title")).alias("value")
    )
    
    query = kafka_output_df.writeStream \
        .outputMode("append") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", broker_url) \
        .option("topic", KafkaTopics.PROCESSED_DATA.value) \
        .option("checkpointLocation", "/tmp/spark_checkpoints/deas_stream_pipeline") \
        .start()
    
    query.awaitTermination()
    
    return spark, data_df

if __name__ == "__main__":
    start_consumer()