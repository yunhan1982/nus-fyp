import asyncio
from datetime import datetime, timezone
from utils.generate_rectangles import generate_rectangles
from utils.generate_student_data import generate_student_data
from core.solution1 import Solution1
from core.solution2 import Solution2
from core.solution3 import Solution3
# from core.xtdb_solution import XTDBSolution

# Generate rectangles
def generate_rectangles_data(
    batch_size=100, 
    total_ids=2_000,
    start_time=datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
    end_time=datetime(2025, 3, 1, 0, 0, 0, tzinfo=timezone.utc),
    num_points_per_id=500
):
    """
    Generate rectangle data in batches of specified size.
    
    Args:
        batch_size: Number of IDs to generate in each batch (default: 10,000)
        total_ids: Total number of IDs to generate (default: 4,000,000)
        start_time: Start time for the data range (default: 2018-01-01)
        end_time: End time for the data range (default: 2025-03-01)
        num_points_per_id: Number of points to generate per ID (default: 500)
    
    Returns:
        Generator that yields batches of rectangles
    """
    # Calculate number of batches
    num_batches = (total_ids + batch_size - 1) // batch_size  # Ceiling division
    
    for batch in range(num_batches):
        # For the last batch, adjust size if needed
        current_batch_size = min(batch_size, total_ids - batch * batch_size)
        if current_batch_size <= 0:
            break
            
        print(f"Generating batch {batch+1}/{num_batches} with {current_batch_size} IDs...")
        
        yield generate_rectangles(
            start_time=start_time,
            end_time=end_time,
            num_ids=current_batch_size,
            num_points_per_id=num_points_per_id,
            generate_data=generate_student_data
        )

# Run solution1 async
async def run_solution(solution, rectangles):
    print(f"Starting {solution.name}...")
    await solution.insert_rectangle_to_collections(rectangles)
    print(f"{solution.name} completed")


# Main execution
async def main():
    start_time = datetime.now()
    print(f"Starting at: {start_time}")


    solutions = [
        # Solution1(), 
        # Solution2(), 
        Solution3()
    ]
    await asyncio.gather(
        *[solution.initialize_collections() for solution in solutions]
    )

    
    # xtdbSolution = XTDBSolution()

    # Process each batch of rectangles
    batch_generator = generate_rectangles_data()
    batch_count = 0
    
    for rectangles_batch in batch_generator:
        batch_count += 1
        print(f"Processing batch {batch_count} with {len(rectangles_batch)} rectangles")
        
        # Run all solutions concurrently for this batch
        await asyncio.gather(
            *[run_solution(solution, rectangles_batch) for solution in solutions]
        )
    
    end_time = datetime.now()
    print(f"All solutions executed successfully")
    print(f"Total execution time: {end_time - start_time}")

if __name__ == "__main__":
    asyncio.run(main())