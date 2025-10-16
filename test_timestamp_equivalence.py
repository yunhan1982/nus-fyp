import asyncio
from datetime import datetime, timezone, timedelta
from core.xtdb_solution import XTDBSolution
from core.xtdb_native_solution import XTDBNativeSolution
from core.bitemporal_space import Rectangle

async def test_timestamp_equivalence():
    """Test that both XTDB solutions handle equivalent timestamps correctly"""
    
    # Create test data with specific timestamps
    test_time = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
    valid_from = datetime(2024, 1, 10, 0, 0, 0, tzinfo=timezone.utc)
    valid_to = datetime(2024, 1, 20, 0, 0, 0, tzinfo=timezone.utc)
    
    # Create a test rectangle
    test_rect = Rectangle(
        data={"payload": {"name": "TestStudent", "age": 25, "attr1": 100, "attr2": 200, "attr3": 300, "attr4": 400}, "id": "test_001"},
        tt_from=test_time,
        tt_to=test_time + timedelta(days=1),
        vt_from=valid_from,
        vt_to=valid_to,
        index=1001
    )
    
    print("Testing timestamp equivalence between XTDB solutions...")
    print(f"Test rectangle - Transaction time: {test_rect.tt_from}")
    print(f"Test rectangle - Valid time: {test_rect.vt_from} to {test_rect.vt_to}")
    
    # Test XTDB v2 Solution
    print("\n=== Testing XTDB v2 Solution ===")
    xtdb_v2 = XTDBSolution()
    try:
        await xtdb_v2.connect()
        await xtdb_v2.initialize_collections()
        
        # Insert test data
        success = await xtdb_v2.insert_data([test_rect], test_rect.tt_from, test_rect.vt_from, "Student")
        print(f"XTDB v2 insert success: {success}")
        
        # Query the data back
        entities = await xtdb_v2.get_all_entities(test_rect.tt_from, test_rect.vt_from, "Student")
        print(f"XTDB v2 entities found: {len(entities)}")
        
    except Exception as e:
        print(f"XTDB v2 error: {e}")
    finally:
        await xtdb_v2.cleanup()
    
    # Test XTDB Native Solution
    print("\n=== Testing XTDB Native Solution ===")
    xtdb_native = XTDBNativeSolution()
    try:
        await xtdb_native.connect()
        await xtdb_native.initialize_collections()
        
        # Insert test data
        await xtdb_native.insert_rectangle_to_collections([test_rect], "Student")
        print("XTDB Native insert completed")
        
        # Query the data back
        entities = await xtdb_native.get_all_entities(test_rect.tt_from, test_rect.vt_from, "Student")
        print(f"XTDB Native entities found: {len(entities)}")
        
    except Exception as e:
        print(f"XTDB Native error: {e}")
    finally:
        await xtdb_native.cleanup()
    
    print("\n=== Test completed ===")
    print("Both solutions should handle the same timestamp data equivalently.")

if __name__ == "__main__":
    asyncio.run(test_timestamp_equivalence())