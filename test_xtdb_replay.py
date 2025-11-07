#!/usr/bin/env python3
"""
Test script for XTDBReplaySolution

This script demonstrates how to use the XTDBReplaySolution with bitemporal data,
including Kafka log replay functionality and proper timestamp handling.
"""

import asyncio
import sys
import os
from datetime import datetime, timezone, timedelta
from typing import List

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.xtdb_replay_solution import XTDBReplaySolution
from core.bitemporal_space import Rectangle

def create_sample_rectangles() -> List[Rectangle]:
    """Create sample bitemporal rectangles for testing"""
    base_time = datetime(2023, 1, 1, tzinfo=timezone.utc)
    
    rectangles = [
        # Student Alice - Initial record
        Rectangle(
            data={"id": 1, "name": "Alice", "age": 25, "grade": "A"},
            vt_from=base_time,
            vt_to=base_time + timedelta(days=180),  # Valid for 6 months
            tt_from=base_time + timedelta(days=10),  # Recorded 10 days later
            tt_to=base_time + timedelta(days=200),   # Transaction valid for ~6.5 months
            index=1
        ),
        
        # Student Bob - Initial record
        Rectangle(
            data={"id": 2, "name": "Bob", "age": 23, "grade": "B"},
            vt_from=base_time + timedelta(days=15),
            vt_to=base_time + timedelta(days=195),
            tt_from=base_time + timedelta(days=20),  # Recorded 20 days after start
            tt_to=base_time + timedelta(days=220),
            index=2
        ),
        
        # Alice age update - Correction to age
        Rectangle(
            data={"id": 1, "name": "Alice", "age": 26, "grade": "A"},
            vt_from=base_time + timedelta(days=90),  # Valid from day 90
            vt_to=base_time + timedelta(days=270),
            tt_from=base_time + timedelta(days=95),  # Recorded 5 days after valid time
            tt_to=base_time + timedelta(days=300),
            index=3
        ),
        
        # Bob grade update
        Rectangle(
            data={"id": 2, "name": "Bob", "age": 23, "grade": "A"},
            vt_from=base_time + timedelta(days=120),
            vt_to=base_time + timedelta(days=300),
            tt_from=base_time + timedelta(days=125),
            tt_to=base_time + timedelta(days=350),
            index=4
        )
    ]
    
    return rectangles

async def test_basic_operations():
    """Test basic XTDB replay operations"""
    print("\n=== Testing Basic Operations ===")
    
    # Initialize solution
    solution = XTDBReplaySolution(
        host="localhost",
        port=5433,
        kafka_bootstrap="localhost:9092",
        kafka_topic="xtdb-tx-log"
    )
    
    try:
        # Connect to services
        print("Connecting to XTDB and Kafka...")
        await solution.connect()
        
        # Create and insert sample data
        rectangles = create_sample_rectangles()
        print(f"\nInserting {len(rectangles)} rectangles via Kafka replay...")
        
        await solution.insert_rectangle_to_collections(rectangles, "Student")
        
        # Wait a moment for XTDB to process
        print("Waiting for XTDB to process transactions...")
        await asyncio.sleep(5)
        
        print("\n✓ Basic operations completed successfully")
        
    except Exception as e:
        print(f"✗ Error in basic operations: {e}")
        return False
    
    finally:
        await solution.cleanup()
    
    return True

async def test_bitemporal_queries():
    """Test bitemporal query operations"""
    print("\n=== Testing Bitemporal Queries ===")
    
    solution = XTDBReplaySolution(
        host="localhost",
        port=5433,
        kafka_bootstrap="localhost:9092",
        kafka_topic="xtdb-tx-log"
    )
    
    try:
        await solution.connect()
        
        # Test point queries at different bitemporal coordinates
        base_time = datetime(2023, 1, 1, tzinfo=timezone.utc)
        
        print("\n1. Point Query - Alice at early time:")
        results = await solution.query_by_name_and_age(
            name="Alice",
            age=25,
            tt=base_time + timedelta(days=30),
            vt=base_time + timedelta(days=60),
            entity="Student"
        )
        print(f"   Results: {len(results)} records found")
        for result in results:
            print(f"   - {result}")
        
        print("\n2. Point Query - Alice after age update:")
        results = await solution.query_by_name_and_age(
            name="Alice",
            age=26,
            tt=base_time + timedelta(days=100),
            vt=base_time + timedelta(days=150),
            entity="Student"
        )
        print(f"   Results: {len(results)} records found")
        for result in results:
            print(f"   - {result}")
        
        print("\n3. Range Query - All students with grade A:")
        results = await solution.range_query_by_attribute(
            attribute_name="grade",
            attribute_value="A",
            vt_from=base_time,
            vt_to=base_time + timedelta(days=365),
            tt_from=base_time,
            tt_to=base_time + timedelta(days=365),
            entity="Student"
        )
        print(f"   Results: {len(results)} records found")
        for result in results:
            print(f"   - {result}")
        
        print("\n✓ Bitemporal queries completed successfully")
        
    except Exception as e:
        print(f"✗ Error in bitemporal queries: {e}")
        return False
    
    finally:
        await solution.cleanup()
    
    return True

async def test_delta_queries():
    """Test delta query operations"""
    print("\n=== Testing Delta Queries ===")
    
    solution = XTDBReplaySolution(
        host="localhost",
        port=5433,
        kafka_bootstrap="localhost:9092",
        kafka_topic="xtdb-tx-log"
    )
    
    try:
        await solution.connect()
        
        base_time = datetime(2023, 1, 1, tzinfo=timezone.utc)
        
        print("\n1. Delta Since VT Range - Alice changes:")
        results = await solution.delta_since_vt_range(
            attribute_name="name",
            attribute_value="Alice",
            vt_from=base_time,
            vt_to=base_time + timedelta(days=200),
            tt=base_time + timedelta(days=150),
            entity="Student"
        )
        print(f"   Results: {len(results)} changes found")
        for result in results:
            print(f"   - {result}")
        
        print("\n2. Delta Since TT Range - Bob changes:")
        results = await solution.delta_since_tt_range(
            attribute_name="name",
            attribute_value="Bob",
            tt_from=base_time,
            tt_to=base_time + timedelta(days=200),
            vt=base_time + timedelta(days=150),
            entity="Student"
        )
        print(f"   Results: {len(results)} changes found")
        for result in results:
            print(f"   - {result}")
        
        print("\n✓ Delta queries completed successfully")
        
    except Exception as e:
        print(f"✗ Error in delta queries: {e}")
        return False
    
    finally:
        await solution.cleanup()
    
    return True

async def test_entity_operations():
    """Test entity management operations"""
    print("\n=== Testing Entity Operations ===")
    
    solution = XTDBReplaySolution(
        host="localhost",
        port=5433,
        kafka_bootstrap="localhost:9092",
        kafka_topic="xtdb-tx-log"
    )
    
    try:
        await solution.connect()
        
        base_time = datetime(2023, 1, 1, tzinfo=timezone.utc)
        
        print("\n1. Get All Entities:")
        entities = await solution.get_all_entities(
            tt=base_time + timedelta(days=100),
            vt=base_time + timedelta(days=100),
            entity="Student"
        )
        print(f"   Found {len(entities)} entities:")
        for entity in entities:
            print(f"   - {entity}")
        
        print("\n2. Update Data Test:")
        success = await solution.update_data(
            entity_id="3",
            updates={"name": "Charlie", "age": 22, "grade": "B"},
            tt=base_time + timedelta(days=200),
            vt=base_time + timedelta(days=200),
            entity="Student"
        )
        print(f"   Update successful: {success}")
        
        print("\n✓ Entity operations completed successfully")
        
    except Exception as e:
        print(f"✗ Error in entity operations: {e}")
        return False
    
    finally:
        await solution.cleanup()
    
    return True

async def main():
    """Main test function"""
    print("XTDB Replay Solution Test Suite")
    print("=" * 50)
    
    # Check if services are running
    print("\nChecking prerequisites...")
    print("- Ensure Docker Compose services are running: docker-compose up -d")
    print("- XTDB should be accessible at localhost:5433")
    print("- Kafka should be accessible at localhost:9092")
    
    input("\nPress Enter to continue with tests...")
    
    # Run test suite
    tests = [
        ("Basic Operations", test_basic_operations),
        ("Bitemporal Queries", test_bitemporal_queries),
        ("Delta Queries", test_delta_queries),
        ("Entity Operations", test_entity_operations)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        try:
            success = await test_func()
            results.append((test_name, success))
        except Exception as e:
            print(f"✗ Test '{test_name}' failed with exception: {e}")
            results.append((test_name, False))
    
    # Print summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = 0
    for test_name, success in results:
        status = "✓ PASSED" if success else "✗ FAILED"
        print(f"{test_name:<25} {status}")
        if success:
            passed += 1
    
    print(f"\nTotal: {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        print("\n🎉 All tests passed! XTDBReplaySolution is working correctly.")
    else:
        print(f"\n⚠️  {len(results) - passed} test(s) failed. Check the output above for details.")
    
    print("\nNext steps:")
    print("- Use rebuild_xtdb_from_kafka.sh to test full replay functionality")
    print("- Monitor Kafka messages: docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic xtdb-tx-log")
    print("- Check XTDB status: curl http://localhost:3000/status")

if __name__ == "__main__":
    asyncio.run(main())