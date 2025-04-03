import asyncio
import motor.motor_asyncio
from datetime import datetime, timezone
from generate_rectangles import generate_rectangles
from generate_student_data import generate_student_data
from solution1 import Solution1
from solution2 import Solution2

# Generate rectangles
def generate_rectangles_data(
    batch_size=100, 
    total_ids=4 * 1_000,
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
    print(f"Starting Solution {solution.id}...")
    await solution.insert_rectangle_to_collections(rectangles)
    print(f"Solution {solution.id} completed")


# Main execution
async def main():
    try: 
        start_time = datetime.now()
        print(f"Starting at: {start_time}")

        client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017/', uuidRepresentation='standard')
    
        solution1 = Solution1(client["Solution1"])
        solution2 = Solution2(client["Solution2"])


        # Process each batch of rectangles
        batch_generator = generate_rectangles_data()
        batch_count = 0
        
        for rectangles_batch in batch_generator:
            batch_count += 1
            print(f"Processing batch {batch_count} with {len(rectangles_batch)} rectangles")
            
            # Run all solutions concurrently for this batch
            await asyncio.gather(
                run_solution(solution1, rectangles_batch),
                run_solution(solution2, rectangles_batch)
            )
        
        end_time = datetime.now()
        print(f"All solutions executed successfully")
        print(f"Total execution time: {end_time - start_time}")
    finally:
        client.close()

if __name__ == "__main__":
    asyncio.run(main())