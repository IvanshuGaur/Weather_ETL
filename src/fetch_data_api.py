import requests


API_KEY = "API_KEY"
API_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"


CITIES = [
    "Vancouver", "Surrey", "Burnaby", "Richmond", "Victoria",
    "Coquitlam", "Kelowna", "Prince George", "Kamloops"
]


FORECAST_DAYS = 14


def fetch_data_api(city, days):
    
    api = f"{API_URL}{city}?unitGroup=metric&key={API_KEY}&contentType=json&include=days"
    response = requests.get(api)
    
    if response.status_code == 200:
        weather_data = response.json()
        if "days" in weather_data:
            return weather_data["days"][:days]
        else:
            raise Exception(f"No 'days' data available in the response for {city}")
    else:
        raise Exception(f"Failed to fetch forecast data for {city}, Status code: {response.status_code}")


def main():
    
    forecast_data = {}
    
    for city in CITIES:
        try:
            print(f" Fetching 14-day weather forecast for {city}...")
            city_forecast = fetch_data_api(city, FORECAST_DAYS)
            forecast_data[city] = city_forecast
            print(f" Successfully fetched forecast data for {city}")
        except Exception as e:
            print(f" {e}")
    
   
    print("\n Weather Forecast Summary:")
    for city, forecast in forecast_data.items():
        print(f"\n City: {city}")
        for day in forecast:
            date = day.get("datetime", "N/A")
            temp = day.get("temp", "N/A")
            conditions = day.get("conditions", "N/A")
            print(f"  {date}: {temp}°C, {conditions}")


if __name__ == "__main__":
    main()
