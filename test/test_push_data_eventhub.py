import unittest
from unittest.mock import patch, MagicMock
from src.push_data_eventhub import push_data_eventhub

class TestPushForecastToEventHub(unittest.TestCase):
    @patch('src.push_data_eventhub.EventHubProducerClient')
    def test_push_data_eventhub_success(self, mock_producer_client):
       
        
        mock_producer_instance = MagicMock()
        mock_producer_client.from_connection_string.return_value = mock_producer_instance
        
        
        mock_batch = MagicMock()
        mock_producer_instance.create_batch.return_value = mock_batch
        mock_batch.try_add.return_value = True 
        
        
        sample_forecast_data = {
            "Vancouver": [
                {"datetime": "2024-06-01", "temp": 22, "conditions": "Clear"},
                {"datetime": "2024-06-02", "temp": 21, "conditions": "Cloudy"}
            ]
        }
        
        
        push_data_eventhub(sample_forecast_data)
        
        
        mock_producer_client.from_connection_string.assert_called_once_with(
            conn_str="Endpoint=sb://bc-weather-namespace.servicebus.windows.net/;SharedAccessKeyName=bc-weather-policy;",
            eventhub_name="bc-weather-eventhub"
        )
        
        
        print("Event Hub push test passed successfully!")


if __name__ == "__main__":
    unittest.main()
