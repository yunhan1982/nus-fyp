import duckdb

def init_database():
    # Connect to DuckDB file in the mounted volume
    conn = duckdb.connect('/app/data/db.duckdb')
    
    try:
        # Create Index_ts table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS Index_ts (
                vref VARCHAR,
                eref VARCHAR,
                tt_from TIMESTAMP,
                tt_to TIMESTAMP,
                vt_from TIMESTAMP,
                vt_to TIMESTAMP
            )
        """)
        
        # Create Index_data table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS Index_data (
                vref VARCHAR PRIMARY KEY,
                name VARCHAR,
                age INTEGER
            )
        """)
        
        # Create indexes
        conn.execute("CREATE INDEX IF NOT EXISTS vref_idx ON Index_ts (vref)")
        conn.execute("CREATE INDEX IF NOT EXISTS eref_idx ON Index_ts (eref)")
        conn.execute("CREATE INDEX IF NOT EXISTS tt_idx ON Index_ts (tt_from, tt_to)")
        conn.execute("CREATE INDEX IF NOT EXISTS vt_idx ON Index_ts (vt_from, vt_to)")
        conn.execute("CREATE INDEX IF NOT EXISTS name_idx ON Index_data (name)")
        conn.execute("CREATE INDEX IF NOT EXISTS age_idx ON Index_data (age)")
        
        print("Successfully initialized DuckDB database!")
        
    except Exception as e:
        print(f"Error initializing database: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    init_database() 