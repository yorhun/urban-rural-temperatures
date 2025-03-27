#!/usr/bin/env python3
"""
Daily ETL pipeline for Urban Heat Island data

This script is designed to be run daily to fetch, process, and load
the latest temperature data for urban-rural location pairs.

When used with AWS:
- Run as a Lambda function
- Triggered by EventBridge Scheduler
- Connects to AWS RDS PostgreSQL instance
"""

import json
import logging
from datetime import datetime, timedelta
import traceback
import sys
import os

# Add the directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import setup_logging, get_db_config, DataConfig
from extract import fetch_historical_weather, get_urban_rural_pairs
from load import get_db_connection, load_locations, load_temperature_data, refresh_materialized_views

# Configure logging for both local and Lambda environments
if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
    # Lambda environment - logs will go to CloudWatch
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
else:
    # Local environment
    logger = setup_logging("daily_pipeline.log")

def process_location_pair(conn, pair, start_date, end_date):
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
        logger.info(f"Fetching data for {urban_name}")
        urban_df = fetch_historical_weather(urban_lat, urban_lon, start_date, end_date)
        
        # Fetch rural data
        logger.info(f"Fetching data for {rural_name}")
        rural_df = fetch_historical_weather(rural_lat, rural_lon, start_date, end_date)
        
        # Load location data to get IDs
        locations = load_locations(conn, [pair])
        urban_id = locations[urban_name]
        rural_id = locations[rural_name]
        
        # Load temperature data
        stats['urban_records'] = load_temperature_data(conn, urban_id, urban_df)
        stats['rural_records'] = load_temperature_data(conn, rural_id, rural_df)
        
        # Transform and analysis can be done through materialized views
        # But we could calculate it directly if needed
        # hourly_df, daily_df = calculate_heat_island_differential(urban_df, rural_df)
        
        stats['status'] = 'success'
        logger.info(f"Successfully processed {urban_name}-{rural_name} pair: " +
                   f"{stats['urban_records']} urban and {stats['rural_records']} rural records")
        
    except Exception as e:
        stats['status'] = 'error'
        stats['error'] = str(e)
        logger.error(f"Error processing {urban_name}-{rural_name} pair: {e}")
        logger.error(traceback.format_exc())
    
    return stats

def run_daily_pipeline(date=None, days_back=3, env=None):
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
    
    logger.info(f"Starting daily pipeline for date range: {start_date} to {end_date}")
    
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
    logger.info(f"Processing {len(location_pairs)} location pairs")
    
    conn = None
    try:
        # Get database connection
        conn = get_db_connection(env)
        
        # Process each location pair
        for pair in location_pairs:
            pair_stats = process_location_pair(conn, pair, start_date, end_date)
            stats['location_pairs'].append(pair_stats)
            
            # Update totals
            if pair_stats['status'] == 'success':
                stats['success_count'] += 1
                stats['total_records'] += pair_stats['urban_records'] + pair_stats['rural_records']
            else:
                stats['error_count'] += 1
        
        # Refresh materialized views to include new data
        if stats['total_records'] > 0:
            logger.info("Refreshing materialized views")
            refresh_materialized_views(conn)
        
        # Final status
        stats['status'] = 'error' if stats['error_count'] > 0 else 'success'
        
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        logger.error(traceback.format_exc())
        stats['status'] = 'error'
        stats['error'] = str(e)
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")
    
    # Calculate duration
    pipeline_end = datetime.now()
    duration = pipeline_end - pipeline_start
    stats['end_time'] = pipeline_end.isoformat()
    stats['duration_seconds'] = duration.total_seconds()
    
    logger.info(f"Pipeline completed with status: {stats['status']}")
    logger.info(f"Processed {stats['total_records']} records in {duration.total_seconds():.2f} seconds")
    
    return stats

def lambda_handler(event, context):
    """
    AWS Lambda handler
    
    Args:
        event: Lambda event object
        context: Lambda context
        
    Returns:
        dict: Response with pipeline statistics
    """
    try:
        # Parse parameters from event
        days_back = event.get('days_back', 3)
        date = event.get('date')
        env = event.get('env', 'prod')  # Default to prod in Lambda
        
        # Run pipeline
        stats = run_daily_pipeline(date, days_back, env)
        
        # Prepare Lambda response
        return {
            'statusCode': 200 if stats['status'] == 'success' else 500,
            'body': json.dumps(stats)
        }
    
    except Exception as e:
        logger.error(f"Lambda execution error: {e}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'error',
                'error': str(e)
            })
        }

if __name__ == "__main__":
    # For local execution
    logger.info("Starting daily pipeline local execution")
    stats = run_daily_pipeline(days_back=3)
    print(json.dumps(stats, indent=2))