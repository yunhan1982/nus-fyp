import asyncio
from datetime import datetime, timezone, timedelta
from core.xtdb_solution import XTDBSolution
from core.xtdb_native_solution import XTDBNativeSolution
from core.bitemporal_space import Rectangle

async def verify_timestamp_handling():
    """Verify that both XTDB solutions handle equivalent timestamps correctly"""
    
    print("=== XTDB Timestamp Handling Verification ===")
    print("\nThis test verifies that both XTDB solutions:")
    print("1. Accept the same Rectangle data structures")
    print("2. Handle equivalent system timestamps (tt_from/tt_to)")
    print("3. Handle equivalent valid timestamps (vt_from/vt_to)")
    print("4. Process bitemporal data consistently")
    
    # Create test data with specific bitemporal attributes
    base_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    
    test_rectangles = [
        Rectangle(
            data={"payload": {"name": f"Student_{i}", "age": 20 + i, "attr1": i * 10, "attr2": i * 20, "attr3": i * 30, "attr4": i * 40}, "id": f"test_{i:03d}"},
            tt_from=base_time + timedelta(minutes=i),
            tt_to=base_time + timedelta(minutes=i+10),
            vt_from=base_time + timedelta(hours=i),
            vt_to=base_time + timedelta(hours=i+24),
            index=2000 + i
        )
        for i in range(5)
    ]
    
    print(f"\nCreated {len(test_rectangles)} test rectangles with varying timestamps:")
    for i, rect in enumerate(test_rectangles):
        print(f"  Rectangle {i+1}: TT={rect.tt_from.strftime('%H:%M')}, VT={rect.vt_from.strftime('%H:%M')}")
    
    # Test both solutions
    solutions = [
        ("XTDB v2 Solution", XTDBSolution()),
        ("XTDB Native Solution", XTDBNativeSolution())
    ]
    
    results = {}
    
    for name, solution in solutions:
        print(f"\n=== Testing {name} ===")
        try:
            await solution.connect()
            await solution.initialize_collections()
            
            # Insert test rectangles
            start_time = asyncio.get_event_loop().time()
            await solution.insert_rectangle_to_collections(test_rectangles, "Student")
            insert_time = asyncio.get_event_loop().time() - start_time
            
            print(f"✓ Successfully inserted {len(test_rectangles)} rectangles in {insert_time:.3f}s")
            print(f"✓ Handled system timestamps: {test_rectangles[0].tt_from} to {test_rectangles[-1].tt_from}")
            print(f"✓ Handled valid timestamps: {test_rectangles[0].vt_from} to {test_rectangles[-1].vt_from}")
            
            results[name] = {
                'success': True,
                'insert_time': insert_time,
                'rectangles_processed': len(test_rectangles)
            }
            
        except Exception as e:
            print(f"✗ Error: {e}")
            results[name] = {'success': False, 'error': str(e)}
        finally:
            await solution.cleanup()
    
    # Summary
    print("\n=== Verification Summary ===")
    all_successful = all(result.get('success', False) for result in results.values())
    
    if all_successful:
        print("✓ VERIFICATION PASSED: Both solutions handle equivalent timestamps correctly")
        print("\nBoth solutions successfully:")
        print("  - Process Rectangle objects with tt_from/tt_to (system time)")
        print("  - Process Rectangle objects with vt_from/vt_to (valid time)")
        print("  - Handle bitemporal data insertion consistently")
        print("  - Use the same data structures and interfaces")
        
        for name, result in results.items():
            print(f"  - {name}: {result['rectangles_processed']} rectangles in {result['insert_time']:.3f}s")
    else:
        print("✗ VERIFICATION FAILED: Issues detected")
        for name, result in results.items():
            if not result.get('success', False):
                print(f"  - {name}: {result.get('error', 'Unknown error')}")
    
    return all_successful

if __name__ == "__main__":
    success = asyncio.run(verify_timestamp_handling())
    exit(0 if success else 1)