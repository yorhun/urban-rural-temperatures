import logging
from datetime import datetime, timedelta
from extract import fetch_historical_weather, get_urban_rural_pairs
from load import get_db_connection, load_locations, load_temperature_data, refresh_materialized_views

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("initial_load.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_historical_data(months_back=6):
    """
    Load initial historical data
    
    Args:
        months_back (int): Number of months of historical data to load
    """
    conn = get_db_connection()
    
    try:
        # Get location pairs
        location_pairs = get_urban_rural_pairs()
        
        # Load locations
        location_ids = load_locations(conn, location_pairs)
        
        # Define date range
        end_date = datetime.now().date() - timedelta(days=1)
        start_date = end_date - timedelta(days=30*months_back)
        
        # For testing purposes, use limited data
        test_start_date = "2025-01-22"
        test_end_date = "2025-03-22"  # 3 months of data
        
        # Process each location pair
        total_records = 0
        for pair in location_pairs:
            urban_name, urban_lat, urban_lon, rural_name, rural_lat, rural_lon = pair
            
            logger.info(f"Processing {urban_name}-{rural_name} pair...")
            
            # Fetch urban data
            logger.info(f"Fetching data for {urban_name}")
            urban_df = fetch_historical_weather(urban_lat, urban_lon, test_start_date, test_end_date)
            
            # Fetch rural data
            logger.info(f"Fetching data for {rural_name}")
            rural_df = fetch_historical_weather(rural_lat, rural_lon, test_start_date, test_end_date)
            
            # Load temperature data
            urban_id = location_ids[urban_name]
            rural_id = location_ids[rural_name]
            
            urban_count = load_temperature_data(conn, urban_id, urban_df)
            rural_count = load_temperature_data(conn, rural_id, rural_df)
            
            logger.info(f"Loaded {urban_count} records for {urban_name}")
            logger.info(f"Loaded {rural_count} records for {rural_name}")
            
            total_records += urban_count + rural_count
        
        # Refresh materialized views
        logger.info("Refreshing materialized views...")
        refresh_materialized_views(conn)
        
        logger.info(f"Initial data load complete. Loaded {total_records} total records.")
        
    except Exception as e:
        logger.error(f"Error during initial data load: {e}")
    finally:
        conn.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    logger.info("Starting initial historical data load...")
    load_historical_data(months_back=3)  # Load 3 months of data for testing