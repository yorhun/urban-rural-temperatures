"""
Configuration module for the Urban Heat Island project.
Centralizes all configuration settings for the entire application.
"""

import os
import logging
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
import json

# Set up base directory
if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
    # In Lambda environment, use /tmp for file operations
    BASE_DIR = Path('/tmp')
else:
    # Local development
    BASE_DIR = Path(__file__).resolve().parent.parent

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
        env (str): Environment name ('dev', 'test', 'prod', None)
                   If None, determined from DB_ENV environment variable
                   
    Returns:
        dict: Database connection parameters
    """
    # Set up logging
    local_logger = logging.getLogger(__name__)
    
    # Determine environment if not provided
    if env is None:
        env = os.environ.get('DB_ENV', 'dev').lower()
    
    # Try to get config from AWS Secrets Manager for production
    if env == 'prod' and os.environ.get('DB_SECRET_NAME'):
        try:
            local_logger.info(f"Getting database credentials from AWS Secrets Manager")
            session = boto3.session.Session()
            client = session.client(service_name='secretsmanager')
            
            secret_name = os.environ.get('DB_SECRET_NAME')
            response = client.get_secret_value(SecretId=secret_name)
            
            secret = json.loads(response['SecretString'])
            
            return {
                'host': secret.get('host'),
                'database': secret.get('dbname', 'heat_island'),
                'user': secret.get('username'),
                'password': secret.get('password'),
                'port': secret.get('port', '5432')
            }
        except Exception as e:
            local_logger.error(f"Error getting secret from AWS Secrets Manager: {e}")
            local_logger.warning("Falling back to environment variables")
    
    # Base configuration for all environments
    config = {
        'host': os.environ.get('DB_HOST', 'localhost'),
        'database': os.environ.get('DB_NAME', 'heat_island'),
        'user': os.environ.get('DB_USER', 'postgres'),
        'password': os.environ.get('DB_PASSWORD', 'postgres'),
        'port': os.environ.get('DB_PORT', '5432')
    }
    
    # Environment-specific overrides
    if env == 'test':
        # Test environment might use different DB
        config['database'] = os.environ.get('TEST_DB_NAME', 'heat_island_test')
    elif env == 'prod':
        # Production settings - require environment variables to be set
        # to avoid accidental use of default credentials in production
        if 'DB_HOST' not in os.environ and 'DB_SECRET_NAME' not in os.environ:
            raise ValueError("DB_HOST or DB_SECRET_NAME environment variable must be set in production")
        if 'DB_PASSWORD' not in os.environ and 'DB_SECRET_NAME' not in os.environ:
            raise ValueError("DB_PASSWORD or DB_SECRET_NAME environment variable must be set in production")
    
    return config

# API configuration
class APIConfig:
    """API configuration settings"""
    # Open-Meteo API endpoint
    WEATHER_API_URL = "https://archive-api.open-meteo.com/v1/archive"
    
    # Default parameters for API requests
    DEFAULT_PARAMS = {
        "hourly": "temperature_2m,relative_humidity_2m,surface_pressure",
        "timezone": "UTC"
    }
    
    # Rate limiting
    MAX_REQUESTS_PER_MINUTE = 10
    REQUEST_TIMEOUT = 30  # seconds
    
    # Add more API settings as needed

# Data processing configuration
class DataConfig:
    """Data processing configuration"""
    # Default parameters
    DEFAULT_DAYS_BACK = 1
    DEFAULT_MONTHS_BACK = 3
    
    # Test parameters
    TEST_START_DATE = "2023-07-01"
    TEST_END_DATE = "2023-07-07"  # One week of test data
    
    # Long test parameters for initial load
    INITIAL_LOAD_START_DATE = "2023-07-01"
    INITIAL_LOAD_END_DATE = "2023-09-30"  # Three months of test data
    
    # File paths
    SCHEMA_PATH = os.path.join(str(BASE_DIR), "db", "sql", "schema.sql")
    INDEXES_PATH = os.path.join(str(BASE_DIR), "db", "sql", "indexes.sql")
    VIEWS_PATH = os.path.join(str(BASE_DIR), "db", "sql", "views.sql")
    
    # Analysis parameters
    HEAT_ISLAND_WINDOW_DAYS = 30  # Rolling window size for heat island intensity