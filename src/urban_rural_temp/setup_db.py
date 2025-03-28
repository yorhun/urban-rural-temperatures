import psycopg2
import logging
import os
from pathlib import Path
from config import get_db_config, setup_logging, DataConfig

# Configure logging
logger = setup_logging("setup_db.log")

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
        logger.info(f"Connecting to PostgreSQL at {db_params['host']}:{db_params['port']}")
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Execute schema.sql
        logger.info(f"Executing schema.sql from {DataConfig.SCHEMA_PATH}")
        if os.path.exists(DataConfig.SCHEMA_PATH):
            with open(DataConfig.SCHEMA_PATH, "r") as f:
                schema_sql = f.read()
                cursor.execute(schema_sql)
        else:
            logger.error(f"Schema file not found at {DataConfig.SCHEMA_PATH}")
            raise FileNotFoundError(f"Schema file not found at {DataConfig.SCHEMA_PATH}")
        
        # Execute indexes.sql if it exists
        if os.path.exists(DataConfig.INDEXES_PATH):
            logger.info(f"Executing indexes.sql from {DataConfig.INDEXES_PATH}")
            with open(DataConfig.INDEXES_PATH, "r") as f:
                indexes_sql = f.read()
                cursor.execute(indexes_sql)
        else:
            logger.warning(f"Indexes file not found at {DataConfig.INDEXES_PATH}")
        
        # Execute views.sql if it exists
        if os.path.exists(DataConfig.VIEWS_PATH):
            logger.info(f"Executing views.sql from {DataConfig.VIEWS_PATH}")
            with open(DataConfig.VIEWS_PATH, "r") as f:
                views_sql = f.read()
                cursor.execute(views_sql)
        else:
            logger.warning(f"Views file not found at {DataConfig.VIEWS_PATH}")
        
        # Commit the changes
        conn.commit()
        logger.info("Database setup completed successfully!")
        
    except Exception as e:
        # Roll back in case of error
        if conn:
            conn.rollback()
        logger.error(f"Error setting up database: {e}")
        raise
        
    finally:
        # Close database connection
        if conn:
            cursor.close()
            conn.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    setup_database()