from datetime import datetime, timedelta
from extract import ExtractData
from config import get_urban_rural_pairs
from load import LoadData
import os

class InitialLoad:
    """
    Class for initial data load
    """
    def __init__(self, test_start_date=os.getenv('TEST_START_DATE', ''), test_end_date=os.getenv('TEST_END_DATE', '')):
        if os.getenv('DB_ENV', '') == 'dev':
            self.start_date = test_start_date
            self.end_date = test_end_date
        else:
            self.start_date = os.getenv('INITIAL_LOAD_START_DATE')
            self.end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.load_data = LoadData()
        self.extract_data = ExtractData()
    def load_historical_data(self):
        """
        Load initial historical data
        """
        conn = self.load_data.get_db_connection()
        
        try:
            # Get location pairs
            location_pairs = get_urban_rural_pairs()
            
            # Load locations
            location_ids = self.load_data.load_locations(conn, location_pairs)

            # Process each location pair
            total_records = 0
            for pair in location_pairs:
                urban_name, urban_lat, urban_lon, rural_name, rural_lat, rural_lon = pair
                
                print(f"Processing {urban_name}-{rural_name} pair...")
                
                # Fetch urban data
                print(f"Fetching data for {urban_name}")
                urban_df = self.extract_data.fetch_historical_weather(urban_lat, urban_lon, self.start_date, self.end_date)
                
                # Fetch rural data
                print(f"Fetching data for {rural_name}")
                rural_df = self.extract_data.fetch_historical_weather(rural_lat, rural_lon, self.start_date, self.end_date)
                
                # Load temperature data
                urban_id = location_ids[urban_name]
                rural_id = location_ids[rural_name]
                
                urban_count = self.load_data.load_temperature_data(conn, urban_id, urban_df)
                rural_count = self.load_data.load_temperature_data(conn, rural_id, rural_df)
                
                print(f"Loaded {urban_count} records for {urban_name}")
                print(f"Loaded {rural_count} records for {rural_name}")
                
                total_records += urban_count + rural_count
            
            # Refresh materialized views
            print("Refreshing materialized views...")
            self.load_data.refresh_materialized_views(conn)
            
            print(f"Initial data load complete. Loaded {total_records} total records.")
            
        except Exception as e:
            print(f"Error during initial data load: {e}")
        finally:
            conn.close()
            print("Database connection closed")

if __name__ == "__main__":
    print("Starting initial historical data load...")
    initial_load = InitialLoad()
    initial_load.load_historical_data() 