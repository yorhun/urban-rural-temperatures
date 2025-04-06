import psycopg2
from psycopg2.extras import execute_values
from config import get_db_config
from utils import create_partition_if_needed
from psycopg2 import pool
import threading


class LoadData:
    """
    Data load class
    """
    def __init__(self):
        self.connection_pool = None
        self.pool_lock = threading.Lock()  # Thread safety for pool creation

    def init_connection_pool(self, env=None, min_connections=1, max_connections=10):
        """
        Initialize a connection pool for database operations
        
        Args:
            env (str): Environment ('dev', 'test', 'prod')
            min_connections (int): Minimum number of connections in the pool
            max_connections (int): Maximum number of connections in the pool
            
        Returns:
            ThreadedConnectionPool: Database connection pool
        """
        
        with self.pool_lock:
            if self.connection_pool is None:
                db_params = get_db_config(env)
                print(f"Initializing connection pool for {db_params['host']}:{db_params['port']}")
                
                self.connection_pool = pool.ThreadedConnectionPool(
                    min_connections,
                    max_connections,
                    host=db_params['host'],
                    database=db_params['database'],
                    user=db_params['user'],
                    password=db_params['password'],
                    port=db_params['port']
                )
                
        return self.connection_pool

    def get_connection_from_pool(self, env=None):
        """
        Get a connection from the pool
        
        Args:
            env (str): Environment ('dev', 'test', 'prod')
            
        Returns:
            connection: Database connection
        """
        
        if self.connection_pool is None:
            self.init_connection_pool(env)
            
        return self.connection_pool.getconn()

    def return_connection_to_pool(self, conn):
        """
        Return a connection to the pool
        
        Args:
            conn: Database connection to return
        """
        
        if self.connection_pool is not None and conn is not None:
            self.connection_pool.putconn(conn)

    def get_db_connection(self, env=None, **override_params):
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
            
            print(f"Creating direct connection to database {config['database']} on {config['host']}:{config['port']}")
            
            return psycopg2.connect(
                host=config['host'],
                database=config['database'],
                user=config['user'],
                password=config['password'],
                port=config['port']
            )
        else:
            # Use connection pool
            return self.get_connection_from_pool(env)

    def load_locations(self, conn, location_pairs):
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
            print(f"Loading {len(location_pairs)} location pairs to database")
            
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
                
                print(f"Added/updated locations: {urban_name} (ID: {urban_id}) and {rural_name} (ID: {rural_id})")
            
            conn.commit()
            print("Successfully loaded all location data")
            
        except Exception as e:
            conn.rollback()
            print(f"Error loading locations: {e}")
            raise
        finally:
            cursor.close()
        
        return location_ids

    def load_temperature_data(self, conn, location_id, temp_df):
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
            print(f"Loading {len(temp_df)} temperature records for location ID {location_id}")
            
            # Check if we need to create new partitions
            if not temp_df.empty:
                min_date = temp_df['timestamp'].min()
                max_date = temp_df['timestamp'].max()
                create_partition_if_needed(conn, min_date)
                create_partition_if_needed(conn, max_date)
            
            # Prepare data for insertion
            records = [
                (location_id, row.timestamp, row.temperature)
                for row in temp_df.itertuples()
            ]
            
            # Batch insert
            if records:
                execute_values(cursor, """
                INSERT INTO temperature_data 
                    (location_id, timestamp, temperature)
                VALUES %s
                ON CONFLICT (location_id, timestamp) DO UPDATE 
                SET temperature = EXCLUDED.temperature
                """, records)
                
                conn.commit()
                print(f"Successfully loaded {len(records)} temperature records")
                return len(records)
            else:
                print("No temperature records to load")
                return 0
                
        except Exception as e:
            conn.rollback()
            print(f"Error loading temperature data: {e}")
            raise
        finally:
            cursor.close()

    def refresh_materialized_views(self, conn):
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
                    "normalized_differential_daily",
                    "normalized_differential_hourly",
                    "normalized_differential",
                    "time_of_day_pattern"
                ]
            
            print(f"Found {len(views)} materialized views to refresh")
            
            for view in views:
                print(f"Refreshing materialized view: {view}")
                
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
                    print(f"Refreshed view: {view}")
                else:
                    print(f"View {view} does not exist, skipping refresh")
            
            conn.commit()
            print("Successfully refreshed all existing materialized views")
            
        except Exception as e:
            conn.rollback()
            print(f"Error refreshing materialized views: {e}")
            raise
        finally:
            cursor.close()

if __name__ == "__main__":
    # Test database connection
    try:
        conn = LoadData.get_db_connection()
        print("Database connection successful")
        conn.close()
    except Exception as e:
        print(f"Database connection failed: {e}")