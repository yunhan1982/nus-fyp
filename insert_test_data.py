#!/usr/bin/env python3
"""
Insert specific test data that matches point_query.py requirements
"""

import asyncio
import sys
import os
from datetime import datetime, timezone

# Add the core directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'core'))

from core.xtdb_solution import XTDBSolution
from core.bitemporal_space import Rectangle

async def insert_matching_data():
    """Insert data that matches point_query.py parameters"""
    print("=== Inserting Test Data for Point Query ===")
    
    # Initialize XTDB solution
    solution = XTDBSolution(
        host="localhost",
        port=5432,
        database="xtdb",
        user="xtdb",
        password="xtdb"
    )
    
    try:
        # Connect and initialize
        await solution.connect()
        await solution.initialize_collections()
        
        # Create the exact rectangle that point_query.py is looking for
        # Query parameters: name="Student_2519", age=11, tt=2018-01-02, vt=2018-01-05
        rectangles = [
            Rectangle(
                index=2519,
                data={
                    "id": 2519,
                    "payload": {
                        "name": "Student_2519",
                        "age": 11,
                        "attr1": 100,
                        "attr2": 200,
                        "attr3": 300,
                        "attr4": 400
                    }
                },
                # Use dates that will be valid for the query
                tt_from=datetime(2018, 1, 1, tzinfo=timezone.utc),
                tt_to=datetime(2018, 12, 31, tzinfo=timezone.utc),
                vt_from=datetime(2018, 1, 1, tzinfo=timezone.utc),
                vt_to=datetime(2018, 12, 31, tzinfo=timezone.utc)
            )
        ]
        
        print(f"Inserting data for Student_2519 with age 11...")
        await solution.insert_rectangle_to_collections(rectangles)
        
        # Verify the data was inserted
        tt = datetime(2018, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
        vt = datetime(2018, 1, 5, 0, 0, 0, tzinfo=timezone.utc)
        
        print(f"\nVerifying data insertion...")
        results = await solution.query_by_name_and_age("Student_2519", 11, tt, vt)
        print(f"Query results: {len(results)} records found")
        
        if results:
            print(f"Success! Found: {results[0]}")
        else:
            print("No records found - there might be a temporal mismatch")
            
        print("\n=== Data insertion completed ===")
        
    except Exception as e:
        print(f"Error during data insertion: {e}")
    finally:
        await solution.cleanup()

if __name__ == "__main__":
    asyncio.run(insert_matching_data())