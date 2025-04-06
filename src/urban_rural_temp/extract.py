import requests
import pandas as pd
from datetime import datetime
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from config import APIConfig


class ExtractData:
    """
    Data extraction class
    """
    def __init__(self, max_retries=3, backoff_factor=0.5, max_rate_per_min=APIConfig.MAX_REQUESTS_PER_MINUTE):
        self.max_retries=max_retries
        self.backoff_factor=backoff_factor
        self.max_rate_per_min=max_rate_per_min

    def fetch_with_retry(self, url, params):
        """
        Fetch data with retry logic and rate limiting
        
        Args:
            url (str): API URL
            params (dict): Query parameters
            max_retries (int): Maximum number of retries
            backoff_factor (float): Backoff factor for retries
            max_rate_per_min (int): Maximum requests per minute
            
        Returns:
            dict: API response JSON
        """
        # Add simple delay for rate limiting if needed
        time.sleep(60 / self.max_rate_per_min)  # Simple rate limiting
        
        # Set up session with retry logic
        session = requests.Session()
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        
        try:
            response = session.get(url, params=params, timeout=APIConfig.REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data after {self.max_retries} retries: {e}")
            raise

    def fetch_historical_weather(self, latitude, longitude, start_date, end_date):
        """
        Fetch historical weather data from Open-Meteo API
        
        Args:
            latitude (float): Location latitude
            longitude (float): Location longitude
            start_date (str or datetime): Start date in format 'YYYY-MM-DD'
            end_date (str or datetime): End date in format 'YYYY-MM-DD'
            
        Returns:
            pandas.DataFrame: Weather data with columns for timestamp and temperature
        """
        # Convert datetime objects to strings if needed
        if isinstance(start_date, datetime):
            start_date = start_date.strftime('%Y-%m-%d')
        if isinstance(end_date, datetime):
            end_date = end_date.strftime('%Y-%m-%d')
        
        
        print(f"Fetching weather data for coordinates ({latitude}, {longitude}) "
                    f"from {start_date} to {end_date}")
        
        url = APIConfig.WEATHER_API_URL
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,  # Use actual input parameters
            "end_date": end_date,      # Use actual input parameters
            "hourly": "temperature_2m",
            "timezone": "UTC"
        }
        
        try:
            data = self.fetch_with_retry(url, params)

            # Check if the response contains the expected data
            if "hourly" not in data:
                print(f"Unexpected API response format. Missing 'hourly' key. Response: {data}")
                raise ValueError("Unexpected API response format")
                
            required_fields = ["time", "temperature_2m"]
            for field in required_fields:
                if field not in data["hourly"]:
                    print(f"Missing required field '{field}' in API response")
                    raise ValueError(f"Missing required field '{field}' in API response")
                
            # Convert to DataFrame
            df = pd.DataFrame({
                "timestamp": pd.to_datetime(data["hourly"]["time"]),
                "temperature": data["hourly"]["temperature_2m"],
            })
            
            # Drop rows with NaN values
            df_clean = df.dropna()
            
            dropped_count = len(df) - len(df_clean)
            
            if dropped_count > 0:
                missing_percentage = (dropped_count / len(df)) * 100
                print(f"Dropped {dropped_count} rows with NaN values ({missing_percentage:.1f}%)")
                if missing_percentage > 20:
                    print(f"High percentage of missing values ({missing_percentage:.1f}%) for coordinates ({latitude}, {longitude})")
            
            if len(df_clean) == 0:
                print("No valid data points returned from API")
                raise ValueError("No valid data returned from the weather API")
            
            print(f"Successfully fetched {len(df_clean)} records")
            return df_clean
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from Open-Meteo: {e}")
            raise
        except (KeyError, ValueError) as e:
            print(f"Error processing API response: {e}")
            raise
        except Exception as e:
            print(f"Unexpected error in fetch_historical_weather: {e}")
            raise