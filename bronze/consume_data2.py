from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType

# 1. Create Spark Session
spark = SparkSession.builder \
    .appName("RedpandaToDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 2. Define the schema of the 'after' data
after_schema = StructType([
    StructField("ID", IntegerType()),
    StructField("ID_salarie", IntegerType()),
    StructField("Date_de_debut", LongType()),
    StructField("Sport_type", StringType()),
    StructField("Distance_m", IntegerType()),
    StructField("Date_de_fin", LongType()),
    StructField("Commentaire", StringType())
])

# 3. Read stream from Redpanda
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "pg_cdc.public.employee_activities") \
  .option("startingOffsets", "latest") \
  .load()

# 4. Parse the JSON and extract the 'after' field
parsed_df = df.select(
    from_json(col("value").cast("string"), after_schema).alias("data")
).select("data.*")

# 5. Convert timestamps (from microseconds to seconds)
final_df = parsed_df.withColumn("Date_de_debut", from_unixtime(col("Date_de_debut") / 1000000)) \
                    .withColumn("Date_de_fin", from_unixtime(col("Date_de_fin") / 1000000))

# 6. Write the stream to a Delta Lake table
query = final_df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "") \
  .start("/path/to/delta/table")

query.awaitTermination()
