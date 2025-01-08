import json
from azure.eventhub import EventHubProducerClient, EventData
from src.fetch_data_api import fetch_data_api, CITIES, FORECAST_DAYS


CONNECTION_STRING = "Endpoint=sb://bc-weather-namespace.servicebus.windows.net/;SharedAccessKeyName=bc-weather-policy;"
EVENT_HUB_NAME = "bc-weather-eventhub"


def push_data_eventhub(data):
    
    try:
        
        producer = EventHubProducerClient.from_connection_string(
            conn_str=CONNECTION_STRING,
            eventhub_name=EVENT_HUB_NAME
        )
        
        for city, forecast in data.items():
            print(f"Sending weather forecast for {city} to Event Hub...")
            event_data_batch = producer.create_batch()
            
            for day in forecast:
                event_json = json.dumps({
                    "city": city,
                    "date": day.get("datetime"),
                    "temp": day.get("temp"),
                    "conditions": day.get("conditions")
                })
                event_data = EventData(event_json)

                try:
                    event_data_batch.add(event_data)
                except ValueError:
                    
                    print(f"Batch full, sending current batch for {city}")
                    producer.send_batch(event_data_batch)
                    event_data_batch = producer.create_batch()
                    event_data_batch.add(event_data)

            
            if len(event_data_batch) > 0:
                print(f" Sending final batch for {city}")
                producer.send_batch(event_data_batch)
            
            print(f"Successfully sent forecast data for {city} to Event Hub")

    except Exception as e:
        print(f"Failed to send data to Azure Event Hub: {e}")
    finally:
        producer.close()


def main():
    
    forecast_data = {}
    for city in CITIES:
        try:
            print(f" Fetching 14-day weather forecast for {city}...")
            city_forecast = fetch_data_api(city, FORECAST_DAYS)
            forecast_data[city] = city_forecast
            print(f"Successfully fetched forecast data for {city}")
        except Exception as e:
            print(f" Error fetching data for {city}: {e}")
    
    print("\n Pushing all forecast data to Azure Event Hub...")
    push_data_eventhub(forecast_data)
    print(" Process completed successfully!")


if __name__ == "__main__":
    main()
