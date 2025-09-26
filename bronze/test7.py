import json
import logging
import signal
import sys
import argparse
from kafka import KafkaConsumer, TopicPartition
import pandas as pd
from datetime import datetime
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

shutdown_flag = False

def reset_consumer_offsets(consumer, topic_name):
    """Reset offsets to beginning for the consumer group - FIXED VERSION"""
    try:
        # Wait for partition assignment (crucial!)
        consumer.poll(timeout_ms=5000)  # Wait up to 5 seconds for assignment
        
        # Get all partitions for the topic
        partitions = consumer.partitions_for_topic(topic_name)
        if not partitions:
            logging.warning(f"No partitions found for topic {topic_name}")
            return False
        
        topic_partitions = [TopicPartition(topic_name, p) for p in partitions]
        
        # Seek to beginning for all partitions
        consumer.seek_to_beginning(*topic_partitions)
        logging.info(f"Reset offsets to beginning for {len(topic_partitions)} partitions")
        return True
        
    except Exception as e:
        logging.error(f"Error resetting offsets: {e}")
        return False

def create_spark_session():
    """Create and configure Spark session with Delta Lake"""
    try:
        spark = SparkSession.builder \
            .appName("RedpandaToDeltaLake") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.warehouse.dir", "file:///C:/temp/spark-warehouse") \
            .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
            .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
            .master("local[1]") \
            .getOrCreate()
        
        # Set log level to avoid verbose output
        spark.sparkContext.setLogLevel("WARN")
        logging.info("Spark session created successfully")
        return spark
    except Exception as e:
        logging.error(f"Failed to create Spark session: {e}")
        return None

def write_batch_to_delta_lake(spark, batch_data, delta_path):
    """Write a batch of data to Delta Lake"""
    try:
        if not batch_data:
            return True
            
        # Convert batch to DataFrame
        df = spark.createDataFrame(batch_data)
        
        # Write to Delta Lake
        df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(delta_path)
        
        logging.info(f"Successfully wrote {len(batch_data)} records to Delta Lake")
        return True
        
    except Exception as e:
        logging.error(f"Error writing batch to Delta Lake: {e}")
        return False

def signal_handler(sig, frame):
    global shutdown_flag
    logging.info("Shutdown signal received. Stopping gracefully...")
    shutdown_flag = True

def process_debezium_message(message_value):
    try:
        message_data = json.loads(message_value)
        payload = message_data.get('payload', {})
        
        after_data = payload.get('after')
        if after_data:
            # Convert timestamps
            if 'Date_de_debut' in after_data:
                after_data['Date_de_debut'] = datetime.fromtimestamp(after_data['Date_de_debut'] / 1000000)
            if 'Date_de_fin' in after_data:
                after_data['Date_de_fin'] = datetime.fromtimestamp(after_data['Date_de_fin'] / 1000000)
            
            # Add processing timestamp
            after_data['processing_timestamp'] = datetime.now()
            
            return after_data
        return None
        
    except Exception as e:
        logging.error(f"Error processing message: {str(e)}")
        return None

# ... [keep the rest of your functions the same until development_consumer]

def development_consumer(reset_offsets=False, consumer_group="dev-consumer-group", 
                        output_csv=None, delta_path=None):
    """Development consumer with batched Delta Lake writing"""
    global shutdown_flag
    
    # Initialize Spark session for Delta Lake
    spark = None
    if delta_path:
        spark = create_spark_session()
        if not spark:
            logging.warning("Delta Lake functionality disabled due to Spark session failure")
            delta_path = None
    
    try:
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Set default paths if not provided
        if output_csv is None:
            output_csv = "C:/temp/employee_activities_dev.csv"
        if delta_path is None:
            delta_path = "file:///C:/temp/delta/employee_activities"
        
        consumer = KafkaConsumer(
            'pg_cdc.public.employee_activities',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='latest',        
            enable_auto_commit=True,           
            group_id=consumer_group,
            consumer_timeout_ms=1000
        )
        
        logging.info(f"Starting consumer with group: {consumer_group}")
        logging.info(f"Reset offsets: {reset_offsets}")
        logging.info(f"CSV output: {output_csv}")
        if delta_path:
            logging.info(f"Delta Lake path: {delta_path}")
        
        # Reset offsets if requested
        if reset_offsets:
            reset_consumer_offsets(consumer, 'pg_cdc.public.employee_activities')
        
        message_count = 0
        file_exists = os.path.exists(output_csv)
        processed_ids = set()
        delta_batch = []  # Batch for Delta Lake writes
        BATCH_SIZE = 10   # Write to Delta Lake every 10 messages
        
        logging.info("Starting message processing...")
        logging.info("Press Ctrl+C to stop the consumer")
        
        while not shutdown_flag:
            raw_messages = consumer.poll(timeout_ms=1000)
            
            if not raw_messages:
                if reset_offsets and message_count > 0:
                    logging.info("No more messages available, ending batch processing")
                    break
                elif message_count == 0:
                    logging.info("Waiting for messages...")
                continue
                
            for topic_partition, messages in raw_messages.items():
                for message in messages:
                    if message.value:
                        processed_data = process_debezium_message(message.value.decode('utf-8'))
                        if processed_data:
                            message_count += 1
                            record_id = processed_data['ID']
                            
                            if record_id not in processed_ids:
                                processed_ids.add(record_id)
                                logging.info(f"Processed ID: {record_id} (Total: {message_count})")
                            
                            # Save to CSV
                            try:
                                csv_data = processed_data.copy()
                                # Convert timestamps to string for CSV
                                for time_field in ['Date_de_debut', 'Date_de_fin', 'processing_timestamp']:
                                    if time_field in csv_data and csv_data[time_field]:
                                        csv_data[time_field] = csv_data[time_field].strftime('%Y-%m-%d %H:%M:%S')
                                
                                df = pd.DataFrame([csv_data])
                                df.to_csv(output_csv, mode='a', header=not file_exists, index=False)
                                if not file_exists:
                                    file_exists = True
                            except Exception as e:
                                logging.error(f"Error writing to CSV: {e}")
                            
                            # Add to Delta batch
                            if delta_path and spark:
                                delta_batch.append(processed_data)
                                
                                # Write batch when it reaches BATCH_SIZE
                                if len(delta_batch) >= BATCH_SIZE:
                                    write_success = write_batch_to_delta_lake(spark, delta_batch, delta_path)
                                    if write_success:
                                        delta_batch = []  # Clear batch
                                    else:
                                        logging.warning("Delta Lake write failed, keeping batch for retry")
        
        # Write any remaining messages in the batch
        if delta_path and spark and delta_batch:
            write_batch_to_delta_lake(spark, delta_batch, delta_path)
        
        consumer.close()
        if spark:
            spark.stop()
        
        logging.info(f"Consumer run completed! Processed {message_count} messages")
        
    except Exception as e:
        logging.error(f"Error in consumer: {str(e)}")
        if 'consumer' in locals():
            consumer.close()
        if spark:
            spark.stop()

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Redpanda Consumer for Employee Activities')
    
    parser.add_argument('--reset', action='store_true',
                       help='Reset consumer offsets to beginning')
    
    parser.add_argument('--group', type=str, default='dev-consumer-group',
                       help='Consumer group ID (default: dev-consumer-group)')
    
    parser.add_argument('--output-csv', type=str, 
                       help='Output CSV file path')
    
    parser.add_argument('--output-delta', type=str,
                       help='Output Delta Lake path')
    
    parser.add_argument('--no-reset', action='store_true',
                       help='Do not reset offsets (default behavior)')
    
    parser.add_argument('--no-delta', action='store_true',
                       help='Disable Delta Lake writing')
    
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_arguments()
    
    # Determine reset behavior
    should_reset = args.reset
    if args.no_reset:
        should_reset = False
    
    # Determine Delta Lake path
    delta_path = args.output_delta if args.output_delta else "delta/employee_activities"
    if args.no_delta:
        delta_path = None
    
    development_consumer(
        reset_offsets=should_reset,
        consumer_group=args.group,
        output_csv=args.output_csv,
        delta_path=delta_path
    )
    
    logging.info("Consumer completed.")
