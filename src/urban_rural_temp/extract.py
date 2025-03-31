import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def fetch_with_retry(url, params, max_retries=3, backoff_factor=0.5, max_rate_per_min=10):
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
    time.sleep(60 / max_rate_per_min)  # Simple rate limiting
    
    # Set up session with retry logic
    session = requests.Session()
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    
    try:
        response = session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data after {max_retries} retries: {e}")
        raise

def fetch_historical_weather(latitude, longitude, start_date, end_date):
    """
    Fetch historical weather data from Open-Meteo API
    
    Args:
        latitude (float): Location latitude
        longitude (float): Location longitude
        start_date (str or datetime): Start date in format 'YYYY-MM-DD'
        end_date (str or datetime): End date in format 'YYYY-MM-DD'
        
    Returns:
        pandas.DataFrame: Weather data with columns for timestamp, temperature, 
                          humidity, and pressure
    """
    # Convert datetime objects to strings if needed
    if isinstance(start_date, datetime):
        start_date = start_date.strftime('%Y-%m-%d')
    if isinstance(end_date, datetime):
        end_date = end_date.strftime('%Y-%m-%d')
    
    
    logger.info(f"Fetching weather data for coordinates ({latitude}, {longitude}) "
                f"from {start_date} to {end_date}")
    
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,  # Use actual input parameters
        "end_date": end_date,      # Use actual input parameters
        "hourly": "temperature_2m,relative_humidity_2m,surface_pressure",
        "timezone": "UTC"
    }
    
    try:
        data = fetch_with_retry(url, params)

        # Check if the response contains the expected data
        if "hourly" not in data:
            logger.error(f"Unexpected API response format. Missing 'hourly' key. Response: {data}")
            raise ValueError("Unexpected API response format")
            
        required_fields = ["time", "temperature_2m", "relative_humidity_2m", "surface_pressure"]
        for field in required_fields:
            if field not in data["hourly"]:
                logger.error(f"Missing required field '{field}' in API response")
                raise ValueError(f"Missing required field '{field}' in API response")
            
        # Convert to DataFrame
        df = pd.DataFrame({
            "timestamp": pd.to_datetime(data["hourly"]["time"]),
            "temperature": data["hourly"]["temperature_2m"],
            "humidity": data["hourly"]["relative_humidity_2m"],
            "pressure": data["hourly"]["surface_pressure"]
        })
        
        # Drop rows with NaN values
        df_clean = df.dropna()
        
        dropped_count = len(df) - len(df_clean)
        
        if dropped_count > 0:
            missing_percentage = (dropped_count / len(df)) * 100
            logger.warning(f"Dropped {dropped_count} rows with NaN values ({missing_percentage:.1f}%)")
            if missing_percentage > 20:
                logger.warning(f"High percentage of missing values ({missing_percentage:.1f}%) for coordinates ({latitude}, {longitude})")
        
        if len(df_clean) == 0:
            logger.error("No valid data points returned from API")
            raise ValueError("No valid data returned from the weather API")
        
        logger.info(f"Successfully fetched {len(df_clean)} records")
        return df_clean
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from Open-Meteo: {e}")
        raise
    except (KeyError, ValueError) as e:
        logger.error(f"Error processing API response: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in fetch_historical_weather: {e}")
        raise


if __name__ == "__main__":
    # Simple test to verify the extraction works
    lat, lon = 40.7128, -74.0060  # New York City
    end_date = datetime.now().date() - timedelta(days=1)
    start_date = end_date - timedelta(days=2)
    
    try:
        df = fetch_historical_weather(lat, lon, start_date, end_date)
        print(f"Fetched {len(df)} records for New York")
        print(df.head())
    except Exception as e:
        print(f"Error during testing: {e}")