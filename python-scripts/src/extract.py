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
    
def get_urban_rural_pairs():
    """
    Define urban-rural location pairs for analysis
    
    Returns:
        list: List of location pairs, each containing 
              [urban_name, urban_lat, urban_lon, rural_name, rural_lat, rural_lon]
    """
    return [
        # Format: [urban_name, urban_lat, urban_lon, rural_name, rural_lat, rural_lon]
        ["Phoenix", 33.4484, -112.0740, "Buckeye", 33.3705, -112.5838], # commenting others for initial test
        # ["Madrid", 40.4168, -3.7038, "Guadalix de la Sierra", 40.7838, -3.6901],
        # ["Melbourne", -37.8136, 144.9631, "Wandin North", -37.7789, 145.4178],
        # ["Seoul", 37.5665, 126.9780, "Namyangju", 37.6360, 127.2143],
        # ["Atlanta", 33.7490, -84.3880, "Powder Springs", 33.8595, -84.6836],
        # ["Chicago", 41.8781, -87.6298, "Yorkville", 41.6400, -88.4476],
        # ["Beijing", 39.9042, 116.4074, "Huairou", 40.3333, 116.6333],
        # ["Cairo", 30.0444, 31.2357, "El Saff", 29.5700, 31.2800],
        # ["Mexico City", 19.4326, -99.1332, "Amecameca", 19.1238, -98.7724],
        # ["Berlin", 52.5200, 13.4050, "Nauen", 52.6080, 12.8790],
        # ["Denver", 39.7392, -104.9903, "Elizabeth", 39.3594, -104.5972],
        # ["Singapore", 1.3521, 103.8198, "Johor Bahru", 1.4927, 103.7414],
        # ["Stockholm", 59.3293, 18.0686, "Vaxholm", 59.4012, 18.3290],
        # ["Montreal", 45.5017, -73.5673, "Saint-Jean-Baptiste", 45.5167, -73.1167],
        # ["Portland", 45.5152, -122.6784, "Estacada", 45.2901, -122.3348],
        # ["Johannesburg", -26.2041, 28.0473, "Magaliesburg", -25.9894, 27.5419],
        # ["Milan", 45.4642, 9.1900, "Abbiategrasso", 45.3989, 8.9189],
        # ["Delhi", 28.6139, 77.2090, "Manesar", 28.3673, 76.9384],
        # ["Paris", 48.8566, 2.3522, "Dammartin-en-Goële", 49.0667, 2.6833],
        # ["Brisbane", -27.4698, 153.0251, "Dayboro", -27.1961, 152.8197]
    ]



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