#!/usr/bin/env python3
"""
Test script for MarkLogic Solution - equivalent to Solution2 but using MarkLogic

This script demonstrates the MarkLogic implementation that mirrors Solution2's
two-collection approach using MarkLogic's document database capabilities.

Requirements:
- MarkLogic Server running on localhost:8000
- Admin user with password 'admin123'
- REST API enabled
"""

import asyncio
from datetime import datetime, timedelta
from core.marklogic_solution import MarkLogicSolution
from core.utils.generate_rectangles import generate_rectangles

def test_marklogic_solution():
    """Test the MarkLogic solution with sample data"""
    print("Testing MarkLogic Solution (equivalent to Solution2)")
    print("=" * 50)
    
    # Initialize solution
    try:
        solution = MarkLogicSolution(
            host='localhost',
            port=8000,
            username='admin',
            password='admin123'
        )
        print(f"✓ Initialized {solution.name}")
    except Exception as e:
        print(f"✗ Failed to initialize MarkLogic solution: {e}")
        print("Please ensure MarkLogic Server is running on localhost:8000")
        return
    
    # Initialize collections
    try:
        solution.initialize_collections()
        print("✓ Collections initialized")
    except Exception as e:
        print(f"✗ Failed to initialize collections: {e}")
        return
    
    # Generate test data
    print("\nGenerating test data...")
    rectangles = generate_rectangles(num_ids=5, num_points_per_id=3)
    print(f"✓ Generated {len(rectangles)} rectangles")
    
    # Insert data
    try:
        solution.insert_rectangle_to_collections(rectangles, "Student")
        print("✓ Data inserted successfully")
    except Exception as e:
        print(f"✗ Failed to insert data: {e}")
        return
    
    # Test queries
    print("\nTesting queries...")
    
    # Test point-in-time query
    try:
        current_time = datetime.now()
        results = solution.query_by_name_and_age(
            "Alice", 20, current_time, current_time
        )
        print(f"✓ Query by name and age returned {len(results)} results")
    except Exception as e:
        print(f"✗ Query by name and age failed: {e}")
    
    # Test get all current data
    try:
        current_time = datetime.now()
        results = solution.get_all_current_data(current_time, current_time)
        print(f"✓ Get all current data returned {len(results)} results")
    except Exception as e:
        print(f"✗ Get all current data failed: {e}")
    
    # Test get current data for entity
    try:
        current_time = datetime.now()
        results = solution.get_current_data_for_entity(
            1, current_time, current_time
        )
        print(f"✓ Get current data for entity returned {len(results)} results")
    except Exception as e:
        print(f"✗ Get current data for entity failed: {e}")
    
    # Test get data history
    try:
        results = solution.get_data_history(1)
        print(f"✓ Get data history returned {len(results)} results")
    except Exception as e:
        print(f"✗ Get data history failed: {e}")
    
    # Test delete operation
    try:
        current_time = datetime.now()
        solution.delete_data(1, current_time, current_time)
        print("✓ Delete operation completed")
    except Exception as e:
        print(f"✗ Delete operation failed: {e}")
    
    print("\nMarkLogic Solution test completed!")
    print("\nNote: This implementation mirrors Solution2's two-collection approach:")
    print("- Index collection: metadata + temporal bounds + indexed fields")
    print("- Payload collection: actual data with unique vrefs")
    print("- Same query patterns and temporal logic as Solution2")

def compare_with_solution2():
    """Compare the approach with Solution2"""
    print("\nComparison with Solution2:")
    print("=" * 30)
    print("Similarities:")
    print("- Two-collection architecture (Index + Payload)")
    print("- MD5-based vref generation for deduplication")
    print("- Same temporal query logic")
    print("- Batch insert operations")
    print("- Identical method signatures")
    
    print("\nDifferences:")
    print("- MarkLogic: Document database with XQuery")
    print("- MongoDB: Document database with aggregation pipeline")
    print("- MarkLogic: REST API + XQuery for operations")
    print("- MongoDB: Native Python driver (motor)")
    print("- MarkLogic: XML/JSON hybrid storage")
    print("- MongoDB: Pure JSON/BSON storage")

if __name__ == "__main__":
    test_marklogic_solution()
    compare_with_solution2()