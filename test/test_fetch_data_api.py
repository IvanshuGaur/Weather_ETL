import unittest
from unittest.mock import patch, MagicMock
from src.fetch_data_api import fetch_data_api, FORECAST_DAYS


class TestWeatherForecastSuccess(unittest.TestCase):
    @patch('requests.get')
    def test_fetch_data_api(self, mock_get):
        #I am only mocking successful API response i.e. status_code == 200
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "days": [
                {"datetime": "2024-06-01", "temp": 22, "conditions": "Clear"},
                {"datetime": "2024-06-02", "temp": 21, "conditions": "Cloudy"}
            ] * 7  
        }
        
        
        mock_get.return_value = mock_response

        
        result = fetch_data_api("Vancouver", FORECAST_DAYS)

        
        self.assertEqual(len(result), 14)  

       
        mock_get.assert_called_once_with(
            "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/Vancouver?unitGroup=metric&key=PNESX2YFJB9SYP6JCEUL6ML5N&contentType=json&include=days"
        )


if __name__ == "__main__":
    unittest.main()
