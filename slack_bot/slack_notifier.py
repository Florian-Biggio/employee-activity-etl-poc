import json
import os
import time
import argparse
import logging
import sys
from datetime import timedelta
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from kafka import KafkaConsumer, TopicPartition
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('redpanda_consumer.log')
    ]
)

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Redpanda Consumer for Employee Activities')
    
    parser.add_argument('--reset', action='store_true',
                       help='Reset consumer offsets to beginning')
    
    return parser.parse_args()

    
def load_env_config():
    """Load environment variables"""
    load_dotenv()
    config = {
        "kafka_brokers": os.getenv("REDPANDA_BROKERS"),
        "slack_token": os.getenv("SLACK_BOT_TOKEN"),
        "slack_channel_id": os.getenv("SLACK_CHANNEL_ID")
    }
    
    # Validate required config
    if not config["kafka_brokers"]:
        logging.error("REDPANDA_BROKERS environment variable is required")
        sys.exit(1)
    if not config["slack_token"]:
        logging.error("SLACK_BOT_TOKEN environment variable is required")
        sys.exit(1)
        
    return config

def load_json_config(path="config.json"):
    """Load JSON configuration"""
    try:
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Config file not found: {path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in config file: {e}")
        sys.exit(1)

# --- Data Transformers ---
def format_duration(seconds):
    """Convert seconds to human-readable duration (French)"""
    delta = timedelta(seconds=seconds)
    parts = []
    
    if delta.days > 0:
        parts.append(f"{delta.days} jour{'s' if delta.days > 1 else ''}")
    if delta.seconds // 3600 > 0:
        parts.append(f"{delta.seconds // 3600} heure{'s' if delta.seconds // 3600 > 1 else ''}")
    if (delta.seconds % 3600) // 60 > 0:
        parts.append(f"{(delta.seconds % 3600) // 60} minute{'s' if (delta.seconds % 3600) // 60 > 1 else ''}")
    
    if not parts:
        return "quelques secondes"
    return ' et '.join(parts)

def get_sport_name(sport_type):
    """Get French sport name from type"""
    sport_names = {
        "V": "vélo",
        "R": "course à pied",
        "S": "natation",
        "M": "randonnée",
        "E": "musculation",
        "T": "tennis",
        "N": "yoga"
    }
    return sport_names.get(sport_type, "activité sportive")

def format_distance(distance_m):
    """Format distance with 3 significant figures, using appropriate unit"""
    # First ensure distance_m is a number
    try:
        distance_num = float(distance_m)
    except (ValueError, TypeError):
        return ""  # Return empty string if conversion fails
    
    if distance_num >= 1000:  # 1 km or more
        distance_km = distance_num / 1000
        return f"{distance_km:.3g} km"
    return f"{distance_num:.3g} m"

def format_slack_message(payload, json_config):
    """Transform Kafka payload -> French motivational message"""
    # Only process inserts (op='c') and skip tombstones
    if payload.get("op") != 'c' or not payload.get("after"):
        return None
    
    activity = payload["after"]
    if not activity:
        return None

    # Get emoji and sport name
    sport_emoji = json_config["sport_emojis"].get(activity["Sport_type"], "🏃")
    sport_name = get_sport_name(activity["Sport_type"])
    
    # Calculate duration in seconds
    duration_seconds = (activity["Date_de_fin"] - activity["Date_de_debut"]) / 1_000_000
    duration_text = format_duration(duration_seconds)
    
    # Start building message parts
    message_parts = []
    
    # Add distance if relevant (exists and > 0)
    if activity.get("Distance_m") and activity["Distance_m"] > 0:
        message_parts.append(f"sur {format_distance(activity['Distance_m'])}")
    
    # Add duration
    message_parts.append(f"en {duration_text}")
    
    # Get comment if exists
    comment = activity.get("Commentaire")
    
    # Create the final message
    base_message = f"{sport_emoji} Bravo {activity['ID_salarie']} ! Tu viens de faire une session de {sport_name} "
    base_message += ' '.join(message_parts) + " !"
    
    if comment:
        # Add comment in parentheses on a new line
        base_message += f"\n\n(Merci pour ton commentaire : \"{comment}\")"
    
    return {
        "channel": json_config["slack"]["channel_id"],
        "text": base_message
    }

# --- Side-Effect Handlers ---
def send_slack_message(client, message):
    """Handle Slack API call"""
    try:
        response = client.chat_postMessage(**message)
        return {"success": True, "response": response}
    except SlackApiError as e:
        return {"success": False, "error": e.response["error"]}


def reset_consumer_offsets(consumer, topic_name):
    """Reset offsets to beginning for the consumer group"""
    try:
        # Wait for partition assignment
        consumer.poll(timeout_ms=5000)
        
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

def run_consumer(kafka_config, slack_client, json_config, reset_offsets=False):
    """Kafka consumer loop with initial buffer for old messages"""

    kafka_topic = json_config["kafka_topic"]

    consumer = KafkaConsumer(
        kafka_topic, 
        bootstrap_servers=kafka_config["kafka_brokers"],  
        auto_offset_reset='earliest',                    
        enable_auto_commit=True,                          
        group_id="slack-notifier-fp",                     
        consumer_timeout_ms=1000                          
    )

    logging.info(f"Starting consumer with group: slack-notifier-fp")
    logging.info(f"Reset offsets: {reset_offsets}")
    logging.info(f"Topic: {kafka_topic}")
    
    # Reset offsets if requested
    if reset_offsets:
        reset_consumer_offsets(consumer, kafka_topic)

    # State tracking
    initial_buffer = True
    buffered_messages = []
    max_buffer_size = 5
    delay = 3  # seconds between buffered messages
    
    try:
        while True:
            # This returns a DICT of TopicPartition: [messages]
            raw_messages = consumer.poll(timeout_ms=1000)
            
            if not raw_messages:
                if initial_buffer and buffered_messages:
                    # When we've processed all initial messages, send the last 5
                    skipped_count = len(buffered_messages) - max_buffer_size

                    send_slack_message(slack_client, {
                        "channel": json_config["slack"]["channel_id"],
                        "text": "Bip Boop, lancement du système !"
                    })
                    
                    if skipped_count > 0:
                        send_slack_message(slack_client, {
                            "channel": json_config["slack"]["channel_id"],
                            "text": f"{skipped_count} messages ont été omis pour ne pas surcharger le canal. Voici les {max_buffer_size} derniers exploits :"
                        })
                    else:
                        send_slack_message(slack_client, {
                            "channel": json_config["slack"]["channel_id"],
                            "text": "Vite ! Voici les exploits accomplis pendant mon absence :"
                        })
                    
                    # Send the last few messages with small delay
                    for message in buffered_messages[-max_buffer_size:]:
                        send_slack_message(slack_client, message)
                        time.sleep(delay)
                    
                    initial_buffer = False
                    logging.info("Initial buffer processed, now listening for new messages...")
                continue
            
            # Process the batch of messages
            for topic_partition, messages in raw_messages.items():
                for message in messages:  # message is a ConsumerRecord object
                    try:
                        # In kafka-python, we don't check message.error like in confluent-kafka
                        # We just try to process the message value
                        message_value = message.value
                        if not message_value:
                            continue
                            
                        payload = json.loads(message_value.decode('utf-8')).get("payload")
                        if not payload:
                            continue
                            
                        if slack_message := format_slack_message(payload, json_config):
                            if initial_buffer:
                                buffered_messages.append(slack_message)
                                logging.info(f"Buffered message #{len(buffered_messages)}")
                            else:
                                result = send_slack_message(slack_client, slack_message)
                                if result["success"]:
                                    logging.info("Successfully sent Slack message")
                                else:
                                    logging.error(f"Failed to send Slack message: {result['error']}")
                                
                    except json.JSONDecodeError as e:
                        logging.error(f"Error decoding message: {e}")
                    except Exception as e:
                        logging.error(f"Error processing message: {e}")
            
            # Commit after processing the batch
            consumer.commit()
            
    except KeyboardInterrupt:
        logging.info("Stopping consumer...")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        consumer.close()
        logging.info("Consumer closed")

if __name__ == "__main__":  
    print("> Loading configurations...")
    args = parse_arguments()
    should_reset = args.reset
    env_config = load_env_config()
    json_config = load_json_config()

    # Initialize Slack client
    print("> Connecting to Slack...")
    slack_client = WebClient(token=env_config["slack_token"])
    
    # Verify Slack connection
    try:
        auth_test = slack_client.auth_test()
        print(f"> Successfully connected to Slack bot: {auth_test['user']} (Team: {auth_test['team']})")
    except SlackApiError as e:
        print(f"> Failed to connect to Slack: {e.response['error']}")
        exit(1)
    
    # Initialize Kafka consumer
    print(f"> Setting up Redpanda consumer for topic: {json_config['kafka_topic']}")
    print(f"> Broker(s): {env_config['kafka_brokers']}")
    
    # Start consumer
    print("> Starting consumer loop...")
    print("──────────────────────────────────────────")
    run_consumer(env_config, slack_client, json_config, should_reset)
