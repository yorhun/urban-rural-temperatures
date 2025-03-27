import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
from config import get_db_config, setup_logging
from utils import create_partition_if_needed
from psycopg2 import pool
import threading

# Set up logging
logger = setup_logging()

# Global connection pool
connection_pool = None
pool_lock = threading.Lock()  # Thread safety for pool creation

def init_connection_pool(env=None, min_connections=1, max_connections=10):
    """
    Initialize a connection pool for database operations
    
    Args:
        env (str): Environment ('dev', 'test', 'prod')
        min_connections (int): Minimum number of connections in the pool
        max_connections (int): Maximum number of connections in the pool
        
    Returns:
        ThreadedConnectionPool: Database connection pool
    """
    global connection_pool
    
    with pool_lock:
        if connection_pool is None:
            db_params = get_db_config(env)
            logger.info(f"Initializing connection pool for {db_params['host']}:{db_params['port']}")
            
            connection_pool = pool.ThreadedConnectionPool(
                min_connections,
                max_connections,
                host=db_params['host'],
                database=db_params['database'],
                user=db_params['user'],
                password=db_params['password'],
                port=db_params['port']
            )
            
    return connection_pool

def get_connection_from_pool(env=None):
    """
    Get a connection from the pool
    
    Args:
        env (str): Environment ('dev', 'test', 'prod')
        
    Returns:
        connection: Database connection
    """
    global connection_pool
    
    if connection_pool is None:
        init_connection_pool(env)
        
    return connection_pool.getconn()

def return_connection_to_pool(conn):
    """
    Return a connection to the pool
    
    Args:
        conn: Database connection to return
    """
    global connection_pool
    
    if connection_pool is not None and conn is not None:
        connection_pool.putconn(conn)

def get_db_connection(env=None, **override_params):
    """
    Get PostgreSQL database connection from the pool
    
    Args:
        env (str, optional): Environment ('dev', 'test', 'prod')
        **override_params: Optional parameters to override defaults
        
    Returns:
        connection: PostgreSQL database connection
    """
    # Check if we should use direct connection (for setup scripts)
    use_direct = override_params.pop('use_direct', False)
    
    if use_direct:
        # Get config with appropriate environment
        config = get_db_config(env)
        
        # Override with any provided parameters
        config.update({k: v for k, v in override_params.items() if v is not None})
        
        logger.info(f"Creating direct connection to database {config['database']} on {config['host']}:{config['port']}")
        
        return psycopg2.connect(
            host=config['host'],
            database=config['database'],
            user=config['user'],
            password=config['password'],
            port=config['port']
        )
    else:
        # Use connection pool
        return get_connection_from_pool(env)

def load_locations(conn, location_pairs):
    """
    Load or update location data to database
    
    Args:
        conn: Database connection
        location_pairs (list): List of urban-rural location pairs
        
    Returns:
        dict: Dictionary mapping location names to their IDs
    """
    cursor = conn.cursor()
    location_ids = {}
    
    try:
        logger.info(f"Loading {len(location_pairs)} location pairs to database")
        
        for pair in location_pairs:
            urban_name, urban_lat, urban_lon, rural_name, rural_lat, rural_lon = pair
            
            # Insert urban location
            cursor.execute("""
            INSERT INTO locations (name, latitude, longitude, is_urban, urban_pair_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (name) DO UPDATE 
            SET latitude = EXCLUDED.latitude, 
                longitude = EXCLUDED.longitude
            RETURNING location_id
            """, (urban_name, urban_lat, urban_lon, True, None))
            
            urban_id = cursor.fetchone()[0]
            location_ids[urban_name] = urban_id
            
            # Insert rural location with reference to urban pair
            cursor.execute("""
            INSERT INTO locations (name, latitude, longitude, is_urban, urban_pair_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (name) DO UPDATE 
            SET latitude = EXCLUDED.latitude, 
                longitude = EXCLUDED.longitude,
                urban_pair_id = EXCLUDED.urban_pair_id
            RETURNING location_id
            """, (rural_name, rural_lat, rural_lon, False, urban_id))
            
            rural_id = cursor.fetchone()[0]
            location_ids[rural_name] = rural_id
            
            logger.info(f"Added/updated locations: {urban_name} (ID: {urban_id}) and {rural_name} (ID: {rural_id})")
        
        conn.commit()
        logger.info("Successfully loaded all location data")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading locations: {e}")
        raise
    finally:
        cursor.close()
    
    return location_ids

def load_temperature_data(conn, location_id, temp_df):
    """
    Load temperature data to database
    
    Args:
        conn: Database connection
        location_id (int): ID of the location
        temp_df (pandas.DataFrame): DataFrame with temperature data
        
    Returns:
        int: Number of records inserted
    """
    cursor = conn.cursor()
    
    try:
        logger.info(f"Loading {len(temp_df)} temperature records for location ID {location_id}")
        
        # Check if we need to create new partitions
        if not temp_df.empty:
            min_date = temp_df['timestamp'].min()
            max_date = temp_df['timestamp'].max()
            create_partition_if_needed(conn, min_date)
            create_partition_if_needed(conn, max_date)
        
        # Prepare data for insertion
        records = [
            (location_id, row.timestamp, row.temperature, 
            row.humidity if 'humidity' in temp_df else None,
            row.pressure if 'pressure' in temp_df else None)
            for row in temp_df.itertuples()
        ]
        
        # Batch insert
        if records:
            execute_values(cursor, """
            INSERT INTO temperature_data 
                (location_id, timestamp, temperature, humidity, pressure)
            VALUES %s
            ON CONFLICT (location_id, timestamp) DO UPDATE 
            SET temperature = EXCLUDED.temperature,
                humidity = EXCLUDED.humidity,
                pressure = EXCLUDED.pressure
            """, records)
            
            conn.commit()
            logger.info(f"Successfully loaded {len(records)} temperature records")
            return len(records)
        else:
            logger.warning("No temperature records to load")
            return 0
            
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading temperature data: {e}")
        raise
    finally:
        cursor.close()

def refresh_materialized_views(conn):
    """
    Refresh all materialized views
    
    Args:
        conn: Database connection
    """
    cursor = conn.cursor()
    
    try:
        # Get all existing materialized views from database
        cursor.execute("""
        SELECT matviewname 
        FROM pg_catalog.pg_matviews
        WHERE schemaname = 'public'
        """)
        
        views = [row[0] for row in cursor.fetchall()]
        
        # Fallback to known views if query returns empty
        if not views:
            views = [
                "urban_rural_hourly",
                "urban_rural_daily",
                "heat_island_intensity_daily",
                "heat_island_intensity_hourly",
                "heat_island_intensity",
                "time_of_day_pattern"
            ]
        
        logger.info(f"Found {len(views)} materialized views to refresh")
        
        for view in views:
            logger.info(f"Refreshing materialized view: {view}")
            
            # Check if materialized view exists
            cursor.execute("""
            SELECT EXISTS (
                SELECT FROM pg_catalog.pg_matviews
                WHERE matviewname = %s
            )
            """, (view,))
            
            exists = cursor.fetchone()[0]
            
            if exists:
                cursor.execute(f"REFRESH MATERIALIZED VIEW {view}")
                logger.info(f"Refreshed view: {view}")
            else:
                logger.warning(f"View {view} does not exist, skipping refresh")
        
        conn.commit()
        logger.info("Successfully refreshed all existing materialized views")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error refreshing materialized views: {e}")
        raise
    finally:
        cursor.close()

class DatabaseTransaction:
    """Context manager for database transactions"""
    
    def __init__(self, conn=None, env=None, auto_commit=True):
        self.conn = conn
        self.env = env
        self.auto_commit = auto_commit
        self.conn_from_pool = False
        
    def __enter__(self):
        if self.conn is None:
            self.conn = get_connection_from_pool(self.env)
            self.conn_from_pool = True
        return self.conn
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            # An exception occurred, rollback
            logger.error(f"Transaction error: {exc_val}")
            self.conn.rollback()
        elif self.auto_commit:
            # No exception and auto_commit is True, commit
            self.conn.commit()
            
        if self.conn_from_pool:
            # Return connection to pool if we got it from there
            return_connection_to_pool(self.conn)
            self.conn = None
            
if __name__ == "__main__":
    # Test database connection
    try:
        conn = get_db_connection()
        logger.info("Database connection successful")
        conn.close()
    except Exception as e:
        logger.error(f"Database connection failed: {e}")