#!/usr/bin/env python3
"""
Debug XTDB temporal queries
"""

import asyncio
from datetime import datetime, timezone
from core.xtdb_solution import XTDBSolution

async def debug_xtdb():
    """Debug XTDB queries to understand why temporal queries return 0 results"""
    print("=== XTDB Debug Session ===")
    
    solution = XTDBSolution(
        host="localhost",
        port=5432,
        database="xtdb",
        user="xtdb",
        password="xtdb"
    )
    
    try:
        await solution.connect()
        
        # First, let's see what data actually exists in the table
        print("\n1. Checking all data in student table (no temporal filters):")
        try:
            query = "SELECT * FROM student LIMIT 10"
            results = await solution.connection.fetch(query)
            print(f"Found {len(results)} total records")
            for i, row in enumerate(results[:3]):
                print(f"  Record {i+1}: {dict(row)}")
        except Exception as e:
            print(f"Error querying all data: {e}")
        
        # Check if our specific record exists
        print("\n2. Checking for Student_2519 (no temporal filters):")
        try:
            query = "SELECT * FROM student WHERE name = 'Student_2519'"
            results = await solution.connection.fetch(query)
            print(f"Found {len(results)} Student_2519 records")
            for row in results:
                print(f"  {dict(row)}")
        except Exception as e:
            print(f"Error querying Student_2519: {e}")
        
        # Test temporal queries step by step
        print("\n3. Testing temporal queries:")
        tt = datetime(2018, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
        vt = datetime(2018, 1, 5, 0, 0, 0, tzinfo=timezone.utc)
        
        # Test valid time only
        print(f"\n3a. Valid time only (FOR VALID_TIME AS OF '{vt.isoformat()}'):")
        try:
            query = f"""
            SELECT * FROM student 
            FOR VALID_TIME AS OF '{vt.isoformat()}'
            WHERE name = 'Student_2519' AND age = 11
            """
            results = await solution.connection.fetch(query)
            print(f"Found {len(results)} records with valid time filter")
            for row in results:
                print(f"  {dict(row)}")
        except Exception as e:
            print(f"Error with valid time query: {e}")
        
        # Test system time only
        print(f"\n3b. System time only (FOR SYSTEM_TIME AS OF '{tt.isoformat()}'):")
        try:
            query = f"""
            SELECT * FROM student 
            FOR SYSTEM_TIME AS OF '{tt.isoformat()}'
            WHERE name = 'Student_2519' AND age = 11
            """
            results = await solution.connection.fetch(query)
            print(f"Found {len(results)} records with system time filter")
            for row in results:
                print(f"  {dict(row)}")
        except Exception as e:
            print(f"Error with system time query: {e}")
        
        # Test both temporal filters
        print(f"\n3c. Both temporal filters:")
        try:
            query = f"""
            SELECT * FROM student 
            FOR VALID_TIME AS OF '{vt.isoformat()}'
            FOR SYSTEM_TIME AS OF '{tt.isoformat()}'
            WHERE name = 'Student_2519' AND age = 11
            """
            results = await solution.connection.fetch(query)
            print(f"Found {len(results)} records with both temporal filters")
            for row in results:
                print(f"  {dict(row)}")
        except Exception as e:
            print(f"Error with both temporal filters: {e}")
        
        # Check what the actual query method does
        print(f"\n4. Testing the actual query_by_name_and_age method:")
        try:
            results = await solution.query_by_name_and_age("Student_2519", 11, tt, vt)
            print(f"Method returned {len(results)} records")
            for row in results:
                print(f"  {row}")
        except Exception as e:
            print(f"Error with query_by_name_and_age method: {e}")
        
    except Exception as e:
        print(f"Connection error: {e}")
    finally:
        await solution.cleanup()

if __name__ == "__main__":
    asyncio.run(debug_xtdb())