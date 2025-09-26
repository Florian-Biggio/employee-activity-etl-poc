import json
import logging
import signal
import sys
from kafka import KafkaConsumer
import pandas as pd
from datetime import datetime
from deltalake import write_deltalake

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('redpanda_consumer.log')
    ]
)

# Global flag for graceful shutdown
shutdown_flag = False

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    global shutdown_flag
    logging.info("Shutdown signal received. Stopping gracefully...")
    shutdown_flag = True

def process_debezium_message(message_value):
    """Extract the 'after' payload from Debezium message"""
    try:
        message_data = json.loads(message_value)
        payload = message_data.get('payload', {})
        
        after_data = payload.get('after')
        if after_data:
            # Convert microsecond timestamps to datetime strings
            if 'Date_de_debut' in after_data:
                after_data['Date_de_debut'] = datetime.fromtimestamp(after_data['Date_de_debut'] / 1000000).strftime('%Y-%m-%d %H:%M:%S')
            if 'Date_de_fin' in after_data:
                after_data['Date_de_fin'] = datetime.fromtimestamp(after_data['Date_de_fin'] / 1000000).strftime('%Y-%m-%d %H:%M:%S')
            
            return after_data
        return None
        
    except json.JSONDecodeError:
        logging.error("Invalid JSON format in message")
        return None
    except Exception as e:
        logging.error(f"Error processing message: {str(e)}")
        return None

def continuous_consumer():
    """Continuous consumer that processes messages as they arrive"""
    global shutdown_flag
    
    try:
        # Set up signal handler for Ctrl+C
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Create Kafka consumer - start from latest offset to only get new messages
        consumer = KafkaConsumer(
            'pg_cdc.public.employee_activities',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='latest',      # Start from the latest offset
            enable_auto_commit=False,        # Don't commit offsets automatically
            group_id='continuous-consumer-group',
            consumer_timeout_ms=1000         # Check for shutdown every second
        )
        
        logging.info("Connected to Redpanda - waiting for new messages...")
        logging.info("Press Ctrl+C to stop the consumer")
        
        message_count = 0
        batch_size = 5  # Process in small batches
        message_batch = []
        
        while not shutdown_flag:
            # Poll for new messages
            raw_messages = consumer.poll(timeout_ms=1000)
            
            for topic_partition, messages in raw_messages.items():
                for message in messages:
                    if message.value:
                        processed_data = process_debezium_message(message.value.decode('utf-8'))
                        if processed_data:
                            message_batch.append(processed_data)
                            message_count += 1
                            logging.info(f"Processed message ID: {processed_data['ID']} (Total: {message_count})")
                            
                            # Print the data to console
                            print(f"New record: ID={processed_data['ID']}, "
                                  f"Employee={processed_data['ID_salarie']}, "
                                  f"Sport={processed_data['Sport_type']}")
            
            # Process batch if we have enough messages or if we're shutting down
            if message_batch and (len(message_batch) >= batch_size or shutdown_flag):
                try:
                    df = pd.DataFrame(message_batch)
                    
                    # Save to Delta Lake (append mode)
                    delta_path = "C:/temp/delta_employee_activities_continuous"
                    write_deltalake(delta_path, df, mode='append')
                    logging.info(f"Saved batch of {len(message_batch)} records to Delta Lake")
                    
                    # Also save to CSV for easy verification
                    csv_path = "C:/temp/employee_activities_continuous.csv"
                    df.to_csv(csv_path, mode='a', header=not pd.io.common.file_exists(csv_path), index=False)
                    logging.info(f"Appended {len(message_batch)} records to CSV")
                    
                    # Clear the batch
                    message_batch = []
                    
                except Exception as e:
                    logging.error(f"Error saving batch: {str(e)}")
        
        # Process any remaining messages before shutdown
        if message_batch:
            try:
                df = pd.DataFrame(message_batch)
                delta_path = "C:/temp/delta_employee_activities_continuous"
                write_deltalake(delta_path, df, mode='append')
                logging.info(f"Saved final batch of {len(message_batch)} records before shutdown")
            except Exception as e:
                logging.error(f"Error saving final batch: {str(e)}")
        
        consumer.close()
        logging.info(f"Consumer stopped. Total messages processed: {message_count}")
        
    except Exception as e:
        logging.error(f"Error in continuous consumer: {str(e)}")
        if 'consumer' in locals():
            consumer.close()

if __name__ == '__main__':
    logging.info("Starting continuous Redpanda consumer...")
    continuous_consumer()
    logging.info("Continuous consumer completed.")