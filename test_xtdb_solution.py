#!/usr/bin/env python3
"""
Test script for XTDB Solution

This script demonstrates the XTDB solution implementation for bitemporal data storage.
It generates sample data and tests the core functionality of the XTDB solution.

Usage:
    python test_xtdb_solution.py

Prerequisites:
    - XTDB server running on localhost:3000 (use docker-compose up xtdb)
    - Required Python packages: aiohttp
"""

import asyncio
import sys
import os
from datetime import datetime, timezone, timedelta

# Add the core directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'core'))

from xtdb_solution import XTDBSolution
from bitemporal_space import Rectangle
from utils.generate_rectangles import generate_rectangles
from utils.generate_student_data import generate_student_data

async def test_xtdb_solution():
    """Test the XTDB solution with sample data"""
    print("=== XTDB Solution Test ===")
    
    # Initialize XTDB solution with v2 parameters
    solution = XTDBSolution(
        host="localhost",
        port=5432,
        database="xtdb",
        user="xtdb",
        password="xtdb"
    )
    
    try:
        # Test connection
        print("Testing XTDB v2 connection...")
        await solution.connect()
        print("✓ Connected to XTDB v2 successfully")
        
        # Connect and initialize
        await solution.initialize_collections()
        
        # Generate sample rectangles
        print("\nGenerating sample data...")
        rectangles = []
        for i in range(10):
            rect = Rectangle(
                index=i,
                data={
                    "id": i,
                    "payload": {
                        "name": f"Student_{i}",
                        "age": 20 + (i % 10),
                        "grade": "A" if i % 2 == 0 else "B"
                    }
                },
                tt_from=datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(days=i),
                tt_to=datetime(2024, 12, 31, tzinfo=timezone.utc),
                vt_from=datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(days=i),
                vt_to=datetime(2024, 12, 31, tzinfo=timezone.utc)
            )
            rectangles.append(rect)
        
        print(f"Generated {len(rectangles)} rectangles")
        
        # Insert data
        print("\nInserting data into XTDB...")
        await solution.insert_rectangle_to_collections(rectangles)
        
        # Test queries
        print("\nTesting queries...")
        
        # Query by name and age
        query_time = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        print(f"\nQuerying for students at {query_time}...")
        
        # Get all entities first
        entities = await solution.get_all_entities(query_time, query_time)
        print(f"Found {len(entities)} unique entities")
        
        # Test query by name and age (using sample data)
        if rectangles:
            sample_rect = rectangles[0]
            sample_name = sample_rect.data.get("payload", {}).get("name")
            sample_age = sample_rect.data.get("payload", {}).get("age")
            
            if sample_name and sample_age:
                print(f"\nQuerying for name='{sample_name}', age={sample_age}...")
                results = await solution.query_by_name_and_age(
                    sample_name, sample_age, query_time, query_time
                )
                print(f"Query results: {len(results)} records found")
                if results:
                    print(f"Sample result: {results[0]}")
        
        # Test get all current data
        print(f"\nGetting all current data at {query_time}...")
        current_data = await solution.get_all_current_data(query_time, query_time)
        print(f"Current data: {len(current_data)} records")
        
        # Test data history
        if entities:
            sample_entity = entities[0]
            print(f"\nGetting history for entity {sample_entity}...")
            history = await solution.get_data_history(sample_entity)
            print(f"History: {len(history)} versions")
            if history:
                print(f"Sample history entry: {history[0]}")
        
        print("\n=== Test completed successfully! ===")
        
    except Exception as e:
        print(f"\nError during test: {e}")
        print("Make sure XTDB v2 is running on localhost:5433")
        print("Run: docker-compose up xtdb")
        
    finally:
        # Cleanup
        await solution.cleanup()

async def test_connection_only():
    """Test just the connection to XTDB"""
    print("=== Testing XTDB Connection ===")
    
    solution = XTDBSolution()
    
    try:
        await solution.connect()
        print("Connection test successful!")
    except Exception as e:
        print(f"Connection failed: {e}")
        print("Make sure XTDB is running on localhost:3000")
        print("Run: docker-compose up xtdb")
    finally:
        await solution.cleanup()

async def main():
    """Main test function"""
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--connection-only":
        await test_connection_only()
    else:
        await test_xtdb_solution()

if __name__ == "__main__":
    asyncio.run(main())