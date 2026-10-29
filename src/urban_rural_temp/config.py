"""
Configuration module to store constants and configurational functions
"""

import os
import logging
from pathlib import Path

import yaml

BASE_DIR = Path(__file__).resolve().parent.parent.parent
LOCATION_PAIRS_PATH = Path(__file__).with_name("location_pairs.yaml")

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
    SCHEMA_PATH = os.path.join(str(BASE_DIR), "sql", "schema.sql")
    INDEXES_PATH = os.path.join(str(BASE_DIR), "sql", "indexes.sql")
    VIEWS_PATH = os.path.join(str(BASE_DIR), "sql", "views.sql")


def get_urban_rural_pairs():
    """
    Load urban-rural location pairs from the YAML configuration file.
    
    Returns:
        list: Location tuples containing urban name and coordinates followed
              by rural name and coordinates.
    """
    with LOCATION_PAIRS_PATH.open(encoding="utf-8") as pairs_file:
        pairs = yaml.safe_load(pairs_file)

    if not isinstance(pairs, list):
        raise ValueError(f"{LOCATION_PAIRS_PATH} must contain a list of location pairs")

    result = []
    for index, pair in enumerate(pairs, start=1):
        if not isinstance(pair, dict):
            raise ValueError(f"Location pair {index} must be a mapping")

        locations = []
        for location_type in ("urban", "rural"):
            location = pair.get(location_type)
            if not isinstance(location, dict):
                raise ValueError(
                    f"Location pair {index} must define a {location_type} mapping"
                )

            name = location.get("name")
            latitude = location.get("latitude")
            longitude = location.get("longitude")
            if not isinstance(name, str) or not name.strip():
                raise ValueError(
                    f"Location pair {index} has an invalid {location_type} name"
                )
            if type(latitude) not in (int, float) or not -90 <= latitude <= 90:
                raise ValueError(
                    f"Location pair {index} has an invalid {location_type} latitude"
                )
            if type(longitude) not in (int, float) or not -180 <= longitude <= 180:
                raise ValueError(
                    f"Location pair {index} has an invalid {location_type} longitude"
                )

            locations.extend((name, latitude, longitude))

        result.append(tuple(locations))

    return result
