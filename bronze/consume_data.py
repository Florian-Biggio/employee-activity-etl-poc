import json
import random
import time
import logging
from pykafka import KafkaClient
from pykafka.exceptions import KafkaException

def process_user_activity(activity):
    """Processes the user activity data, transforming it and filtering specific events."""
    # Transform timestamp into human-readable format
    activity['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(activity['timestamp']))
    
    # Filter out "view" events (e.g., only care about click/purchase/login)
    if activity['activity'] != 'view':
        logging.info(f"Processed activity: {activity}")
        return activity
    else:
        logging.debug(f"Ignoring activity: {activity}")
        return None

def consume_and_process_data(client, topic_name):
    """Consumes user activity from Redpanda topic, processes it, and writes to a processed topic."""
    try:
        # Get the topics and prepare a consumer and producer
        source_topic = client.topics[topic_name.encode('utf-8')]
        consumer = source_topic.get_simple_consumer()

        batch_size = 5  # Process in batches for efficiency
        message_batch = []

        for message in consumer:
            if message is not None:
                try:
                    activity = json.loads(message.value.decode('utf-8'))
                    processed_activity = process_user_activity(activity)
                    
                    if processed_activity:
                        message_batch.append(processed_activity)
                    
                    # add the message to a .csv


                except json.JSONDecodeError:
                    logging.error(f"Invalid JSON format in message: {message.value}")
                except Exception as e:
                    logging.error(f"Error processing message: {str(e)}")

    except KafkaException as e:
        logging.error(f"Error consuming/producing data from/to Redpanda: {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")

if __name__ == '__main__':
    try:
        # Connect to Redpanda
        client = KafkaClient(hosts='localhost:9092')
        logging.info("Connected to Redpanda")

        # Define topic names
        source_topic = 'pg_cdc.public.employee_activities'

        # Start data consumption and processing
        logging.info("Starting data consumption and processing...")
        consume_and_process_data(client, source_topic)

    except Exception as e:
        logging.error(f"Fatal error in main execution: {str(e)}")