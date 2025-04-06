"""
Daily ETL pipeline

This script is designed to be run daily to fetch, process, and load
the latest temperature data for urban-rural location pairs.

"""

import json
from datetime import datetime, timedelta
import traceback
import sys
import os

# Add the directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import get_urban_rural_pairs
from extract import ExtractData
from load import LoadData



def process_location_pair(extract_data, load_data, conn, pair, start_date, end_date):
    """
    Process a single urban-rural location pair
    
    Args:
        conn: Database connection
        pair: Location pair (urban_name, urban_lat, urban_lon, rural_name, rural_lat, rural_lon)
        start_date: Start date for data collection
        end_date: End date for data collection
        
    Returns:
        dict: Statistics about processed records
    """
    urban_name, urban_lat, urban_lon, rural_name, rural_lat, rural_lon = pair
    stats = {
        'urban_name': urban_name,
        'rural_name': rural_name,
        'urban_records': 0,
        'rural_records': 0,
        'status': 'pending'
    }
    
    try:
        # Fetch urban data
        print(f"Fetching data for {urban_name}")
        urban_df = extract_data.fetch_historical_weather(urban_lat, urban_lon, start_date, end_date)
        
        # Fetch rural data
        print(f"Fetching data for {rural_name}")
        rural_df = extract_data.fetch_historical_weather(rural_lat, rural_lon, start_date, end_date)
        
        # Load location data to get IDs
        locations = load_data.load_locations(conn, [pair])
        urban_id = locations[urban_name]
        rural_id = locations[rural_name]
        
        # Load temperature data
        stats['urban_records'] = load_data.load_temperature_data(conn, urban_id, urban_df)
        stats['rural_records'] = load_data.load_temperature_data(conn, rural_id, rural_df)
        
        
        stats['status'] = 'success'
        print(f"Successfully processed {urban_name}-{rural_name} pair: " +
                   f"{stats['urban_records']} urban and {stats['rural_records']} rural records")
        
    except Exception as e:
        stats['status'] = 'error'
        stats['error'] = str(e)
        print(f"Error processing {urban_name}-{rural_name} pair: {e}")
        print(traceback.format_exc())
    
    return stats

def run_daily_pipeline(extract_data, load_data, date=None, days_back=3, env=None):
    """
    Run the full daily ETL pipeline
    
    Args:
        date (datetime or str, optional): Specific date to process
                                         Default is None (yesterday)
        days_back (int): Number of days to look back if date is None
        env (str, optional): Environment to use ('dev', 'test', 'prod')
        
    Returns:
        dict: Pipeline execution statistics
    """
    pipeline_start = datetime.now()
    
    # Calculate date range
    if date is None:
        end_date = datetime.now().date() - timedelta(days=1)  # Yesterday
        start_date = end_date - timedelta(days=days_back-1)
    else:
        if isinstance(date, str):
            date = datetime.strptime(date, '%Y-%m-%d').date()
        end_date = date
        start_date = end_date - timedelta(days=days_back-1)
    
    print(f"Starting daily pipeline for date range: {start_date} to {end_date}")
    
    # Initialize stats
    stats = {
        'start_time': pipeline_start.isoformat(),
        'date_range': {
            'start': start_date.isoformat(),
            'end': end_date.isoformat()
        },
        'location_pairs': [],
        'total_records': 0,
        'success_count': 0,
        'error_count': 0,
        'status': 'running'
    }
    
    # Get location pairs
    location_pairs = get_urban_rural_pairs()
    print(f"Processing {len(location_pairs)} location pairs")
    
    conn = None
    try:
        # Get database connection
        conn = load_data.get_db_connection(env)
        
        # Process each location pair
        for pair in location_pairs:
            pair_stats = process_location_pair(extract_data, load_data, conn, pair, start_date, end_date)
            stats['location_pairs'].append(pair_stats)
            
            # Update totals
            if pair_stats['status'] == 'success':
                stats['success_count'] += 1
                stats['total_records'] += pair_stats['urban_records'] + pair_stats['rural_records']
            else:
                stats['error_count'] += 1
        
        # Refresh materialized views to include new data
        if stats['total_records'] > 0:
            print("Refreshing materialized views")
            load_data.refresh_materialized_views(conn)
        
        # Final status
        stats['status'] = 'error' if stats['error_count'] > 0 else 'success'
        
    except Exception as e:
        print(f"Pipeline error: {e}")
        print(traceback.format_exc())
        stats['status'] = 'error'
        stats['error'] = str(e)
    finally:
        if conn:
            conn.close()
            print("Database connection closed")
    
    # Calculate duration
    pipeline_end = datetime.now()
    duration = pipeline_end - pipeline_start
    stats['end_time'] = pipeline_end.isoformat()
    stats['duration_seconds'] = duration.total_seconds()
    
    print(f"Pipeline completed with status: {stats['status']}")
    print(f"Processed {stats['total_records']} records in {duration.total_seconds():.2f} seconds")
    
    return stats


if __name__ == "__main__":
    # For local execution
    print("Starting daily pipeline local execution")
    extract_data = ExtractData()
    load_data = LoadData()
    stats = run_daily_pipeline(extract_data, load_data, days_back=3)
    print(json.dumps(stats, indent=2))