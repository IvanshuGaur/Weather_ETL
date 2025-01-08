import os
import csv
import json
from azure.eventhub import EventHubConsumerClient


CONNECTION_STRING = "Endpoint=sb://bc-weather-namespace.servicebus.windows.net/;SharedAccessKeyName=bc-weather-policy;"
EVENT_HUB_NAME = "bc-weather-eventhub"
CONSUMER_GROUP = "$Default"


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
CSV_DIR = os.path.join(BASE_DIR, 'data')
os.makedirs(CSV_DIR, exist_ok=True)
CSV_FILE = os.path.join(CSV_DIR, 'weather_forecast_data.csv')


if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["city", "date", "temp", "conditions"])  


def on_event(partition_context, event):
    
    try:
        print(f"Received event from partition: {partition_context.partition_id}")
        event_data = event.body_as_str()
        print(f"Event Data: {event_data}")

        
        event_json = json.loads(event_data)

        
        with open(CSV_FILE, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([
                event_json.get("city", "N/A"),
                event_json.get("date", "N/A"),
                event_json.get("temp", "N/A"),
                event_json.get("conditions", "N/A")
            ])
        
        # Update checkpoint
        partition_context.update_checkpoint(event)
        print("Data written to CSV and checkpoint updated.")
    
    except json.JSONDecodeError:
        print("Failed to parse event data as JSON.")
    except Exception as e:
        print(f"Error processing event: {e}")


def on_error(partition_context, error):
    
    if partition_context:
        print(f" Error on partition {partition_context.partition_id}: {error}")
    else:
        print(f"General Error: {error}")


def on_partition_initialize(partition_context):
    
    print(f"Partition {partition_context.partition_id} initialized.")


def on_partition_close(partition_context, reason):
    
    print(f" Partition {partition_context.partition_id} closed. Reason: {reason}")


def read_new_events_from_eventhub():
    
    try:
        consumer = EventHubConsumerClient.from_connection_string(
            conn_str=CONNECTION_STRING,
            consumer_group=CONSUMER_GROUP,
            eventhub_name=EVENT_HUB_NAME
        )
        
        print("Starting to receive new events from Event Hub")
        with consumer:
            consumer.receive(
                on_event=on_event,
                on_error=on_error,
                on_partition_initialize=on_partition_initialize,
                on_partition_close=on_partition_close,
                starting_position="@latest"  
            )
    except KeyboardInterrupt:
        print("Stopped reading events from Event Hub.")
    except Exception as e:
        print(f"Failed to read events from Event Hub: {e}")


if __name__ == "__main__":
    read_new_events_from_eventhub()
