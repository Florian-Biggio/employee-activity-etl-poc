# slack_notifier_fp.py
import json
import os
import time
from datetime import timedelta
from confluent_kafka import Consumer
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv

# --- Config Loaders ---
def load_env_config():
    """Load environment variables"""
    load_dotenv()
    return {
        "kafka_brokers": os.getenv("REDPANDA_BROKERS"),
        "slack_token": os.getenv("SLACK_BOT_TOKEN"),
        "slack_channel_id": os.getenv("SLACK_CHANNEL_ID")
    }

def load_json_config(path="config.json"):
    """Load JSON configuration"""
    with open(path, encoding='utf-8') as f:
        return json.load(f)

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

# --- Main Pipeline ---
def run_consumer(kafka_config, slack_client, json_config):
    """Kafka consumer loop with initial buffer for old messages"""
    consumer = Consumer({
        "bootstrap.servers": kafka_config["kafka_brokers"],
        "group.id": "slack-notifier-fp",
        "auto.offset.reset": "earliest"
    })
    
    consumer.subscribe([json_config["kafka_topic"]])
    
    # State tracking
    initial_buffer = True
    buffered_messages = []
    max_buffer_size = 5
    delay = 3  # seconds between buffered messages
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
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
                continue
                
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            
            try:
                message_value = msg.value()
                if not message_value:
                    continue
                    
                payload = json.loads(message_value.decode('utf-8')).get("payload")
                if not payload:
                    continue
                    
                if message := format_slack_message(payload, json_config):
                    if initial_buffer:
                        buffered_messages.append(message)
                    else:
                        send_slack_message(slack_client, message)
                        
            except json.JSONDecodeError as e:
                print(f"Error decoding message: {e}")
            except Exception as e:
                print(f"Error processing message: {e}")
            
            consumer.commit(msg)
            
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    # Load configurations
    print("> Loading configurations...")
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
    run_consumer(env_config, slack_client, json_config)
