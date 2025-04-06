import psycopg2
import os
from config import get_db_config, SQLConfig

def setup_database(env=None):
    """
    Set up the database schema directly
    
    Args:
        env (str, optional): Environment ('dev', 'test', 'prod')
    """
    # Get database connection parameters
    db_params = get_db_config(env)
    
    conn = None
    
    try:
        # Connect to PostgreSQL
        print(f"Connecting to PostgreSQL at {db_params['host']}:{db_params['port']}")
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Execute schema.sql
        print(f"Executing schema.sql from {SQLConfig.SCHEMA_PATH}")
        if os.path.exists(SQLConfig.SCHEMA_PATH):
            with open(SQLConfig.SCHEMA_PATH, "r") as f:
                schema_sql = f.read()
                cursor.execute(schema_sql)
        else:
            print(f"Schema file not found at {SQLConfig.SCHEMA_PATH}")
            raise FileNotFoundError(f"Schema file not found at {SQLConfig.SCHEMA_PATH}")
        
        # Execute indexes.sql if it exists
        if os.path.exists(SQLConfig.INDEXES_PATH):
            print(f"Executing indexes.sql from {SQLConfig.INDEXES_PATH}")
            with open(SQLConfig.INDEXES_PATH, "r") as f:
                indexes_sql = f.read()
                cursor.execute(indexes_sql)
        else:
            print(f"Indexes file not found at {SQLConfig.INDEXES_PATH}")
        
        # Execute views.sql if it exists
        if os.path.exists(SQLConfig.VIEWS_PATH):
            print(f"Executing views.sql from {SQLConfig.VIEWS_PATH}")
            with open(SQLConfig.VIEWS_PATH, "r") as f:
                views_sql = f.read()
                cursor.execute(views_sql)
        else:
            print(f"Views file not found at {SQLConfig.VIEWS_PATH}")
        
        # Commit the changes
        conn.commit()
        print("Database setup completed successfully!")
        
    except Exception as e:
        # Roll back in case of error
        if conn:
            conn.rollback()
        print(f"Error setting up database: {e}")
        raise
        
    finally:
        # Close database connection
        if conn:
            cursor.close()
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    setup_database()