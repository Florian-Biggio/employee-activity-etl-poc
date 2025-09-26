from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

# Initialize Spark with Redpanda config
spark = SparkSession.builder \
    .appName("RedpandaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Read from Redpanda
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "pg_cdc.public.employee_activities") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON payload
schema = """your_avro_schema_here""" 
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Batch processing (runs every 5 minutes)
def process_batch(batch_df, batch_id):
    # Your consolidation logic here
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://data-warehouse:5432/dw") \
        .option("dbtable", "consolidated_activities") \
        .mode("append") \
        .save()

parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime="5 minutes") \
    .start() \
    .awaitTermination()
