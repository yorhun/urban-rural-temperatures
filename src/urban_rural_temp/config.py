"""
Configuration module to store constants and configurational functions
"""

import os
import logging
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Configure logging
def setup_logging(log_file=None):
    """Set up logging configuration"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    log_level = os.environ.get('LOG_LEVEL', 'INFO')
    
    handlers = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=getattr(logging, log_level),
        format=log_format,
        handlers=handlers
    )
    
    # Return logger but also configure root logger
    return logging.getLogger(__name__)

# Database configuration
def get_db_config(env=None):
    """
    Get database configuration parameters based on environment
    
    Args:
        env (str): Environment name ('dev', 'prod', None)
                   If None, determined from DB_ENV environment variable
                   
    Returns:
        dict: Database connection parameters
    """

    # Determine environment if not provided
    if env is None:
        env = os.environ.get('DB_ENV', 'dev').lower()
    
    
    # Base configuration for all environments
    config = {
        'host': os.environ.get('DB_HOST', 'localhost'),
        'database': os.environ.get('DB_NAME', 'temperature_diff'),
        'user': os.environ.get('DB_USER', 'postgres'),
        'password': os.environ.get('DB_PASSWORD', 'postgres'),
        'port': os.environ.get('DB_PORT', '5432')
    }
    
    if env == 'prod':
        # Production settings - require environment variables to be set
        # to avoid accidental use of default credentials in production
        if 'DB_HOST' not in os.environ:
            raise ValueError("DB_HOST environment variable must be set in production")
        if 'DB_PASSWORD' not in os.environ or 'DB_USER' not in os.environ:
            raise ValueError("DB_PASSWORD and DB_USER environment variables must be set in production")

    return config

# API configuration
class APIConfig:
    """API configuration settings"""
    # Open-Meteo API endpoint
    WEATHER_API_URL = "https://archive-api.open-meteo.com/v1/archive"

    # Rate limiting
    MAX_REQUESTS_PER_MINUTE = 10
    REQUEST_TIMEOUT = 30  # seconds
    
    # Add more API settings as needed

# Data processing configuration
class SQLConfig:
    """Data processing configuration"""
    
    # File paths
    SCHEMA_PATH = os.path.join(str(BASE_DIR), "db", "sql", "schema.sql")
    INDEXES_PATH = os.path.join(str(BASE_DIR), "db", "sql", "indexes.sql")
    VIEWS_PATH = os.path.join(str(BASE_DIR), "db", "sql", "views.sql")


def get_urban_rural_pairs():
    """
    Define urban-rural location pairs for analysis
    
    Returns:
        list: List of location pairs, each containing 
              [urban_name, urban_lat, urban_lon, rural_name, rural_lat, rural_lon]
    """
    return [
        # Format: [urban_name, urban_lat, urban_lon, rural_name, rural_lat, rural_lon]
        ['Phoenix', 33.427063,-112.02719, 'Buckeye (AZ, USA)', 33.356766,-112.55556],
        ['Madrid', 40.386642,-3.6760864, 'Cotos de Monterrey (SP)', 40.808434,-3.5795593],
        ['Melbourne', -37.785587,144.93976, 'Wandin North (AU)', -37.785587,145.42169],
        ['Seoul', 37.57469,126.96, 'Namyangju (KR)', 37.64499,127.249664],
        ['Atlanta', 33.778557,-84.51492, 'Powder Springs (GA, USA)', 33.848858,-84.73224],
        ['Chicago', 41.862915,-87.64877, 'Yorkville (IL, USA)', 41.65202,-88.43933],
        ['Beijing', 39.89455,116.35983, 'Huairou (CN)', 40.31634,116.58228],
        ['Cairo', 30.052723,31.190199, 'El Saff (EG)', 29.56063,31.25],
        ['Mexico City', 19.437609,-99.10715, 'Amecameca (MX)', 19.156414,-98.80435],
        ['Berlin', 52.54833,13.407822, 'Nauen (DE)', 52.618626,12.929104],
        ['Denver', 39.753952,-105.02086, 'Elizabeth (CO, USA)', 39.33216,-104.64827],
        ['Stockholm', 59.297012,18.163265, 'Gustavsberg (SE)', 59.36731,18.40909],
        ['Montreal', 45.51845,-73.61069, 'La Vallée-du-Richelieu (CA)', 45.51845,-73.18683],
        ['Portland', 45.51845,-122.63736, 'Beavercreek (OR, USA)', 45.307556,-122.484375],
        ['Johannesburg', -26.186293,28.026318, 'Magaliesburg (South Africa)', -25.975395,27.540985],
        ['Milan', 45.448154,9.169279, 'Abbiategrasso (ITA)', 45.377853,8.8732395],
        ['Delhi', 28.646748,77.17218, 'Manesar (IN)', 28.365553,76.92395],
        ['Paris', 48.822495,2.2881355, 'Montge-en-Göele (FR)', 49.03339,2.7597957],
        ['Brisbane', -27.45167,153.02014, 'King Scrub (AU)', -27.170475,152.83965],
        ['Sariyer', 41.08963,29.057144, 'Catalca (TR)', 41.159927,28.454935],
    ]
