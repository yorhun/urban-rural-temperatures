"""
Utility function(s), may be expanded later 
"""

def create_partition_if_needed(conn, timestamp):
    """
    Create a new partition for the temperature_data table if needed
    
    Args:
        conn: Database connection
        timestamp (datetime): Timestamp to check
    """
    # Determine which quarter the timestamp falls into
    year = timestamp.year
    quarter = (timestamp.month - 1) // 3 + 1
    
    # Calculate the start and end dates for the quarter
    start_month = (quarter - 1) * 3 + 1
    start_date = f"{year}-{start_month:02d}-01"
    
    if quarter == 4:
        end_date = f"{year + 1}-01-01"
    else:
        end_date = f"{year}-{(quarter * 3 + 1):02d}-01"
    
    partition_name = f"temperature_data_{year}_q{quarter}"
    
    cursor = conn.cursor()
    
    try:
        # Check if partition exists
        # query database metadata
        cursor.execute("""
        SELECT COUNT(*) FROM pg_tables 
        WHERE tablename = %s AND schemaname = 'public'
        """, (partition_name.lower(),))
        
        result = cursor.fetchone()
        exists = result[0] > 0 if result else False
        
        if not exists:
            # Create the partition if it doesn't exist
            print(f"Creating new partition {partition_name}")
            cursor.execute(f"""
            CREATE TABLE {partition_name} PARTITION OF temperature_data
            FOR VALUES FROM ('{start_date}') TO ('{end_date}')
            """)
            conn.commit()
            
    except Exception as e:
        conn.rollback()
        print(f"Error creating partition: {e}")
        raise
    finally:
        cursor.close()