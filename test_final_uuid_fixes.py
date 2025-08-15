import asyncio
import sys
sys.path.append('/Users/joeng03/Documents/fyp')

from core.solutionA1 import SolutionA1
from core.solutionA2 import SolutionA2
from core.solutionA3 import SolutionA3
from core.solutionB1 import SolutionB1
from core.solutionB2 import SolutionB2
from core.utils.generate_rectangles import generate_rectangles

async def test_uuid_fixes():
    """Test UUID encoding fixes across all MongoDB solutions."""
    solutions = [
        SolutionA1(),
        SolutionA2(),
        SolutionA3(),
        SolutionB1(),
        SolutionB2()
    ]
    
    # Generate small test data
    from core.utils.generate_student_data import generate_student_data
    from datetime import datetime, timezone
    
    start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_time = datetime(2024, 12, 31, tzinfo=timezone.utc)
    rectangles = generate_rectangles(
        start_time=start_time,
        end_time=end_time, 
        num_ids=2,  # Generate 2 student IDs
        num_points_per_id=3,  # 3 data points per student
        generate_data=generate_student_data
    )
    
    for solution in solutions:
        try:
            print(f"\nTesting {solution.name}...")
            
            # Initialize collections
            await solution.initialize_collections()
            print(f"✓ {solution.name}: Collections initialized successfully")
            
            # Insert test data
            await solution.insert_rectangle_to_collections(rectangles)
            print(f"✓ {solution.name}: Data inserted successfully")
            
            # Test query
            results = await solution.query_by_name_and_age("Alice", 25, 1640995200, 1640995200)
            print(f"✓ {solution.name}: Query executed successfully, found {len(results)} results")
            
        except Exception as e:
            print(f"✗ {solution.name}: Failed with error: {e}")
            return False
    
    print("\n🎉 All UUID encoding fixes verified successfully!")
    return True

if __name__ == "__main__":
    asyncio.run(test_uuid_fixes())