import asyncio
from datetime import datetime, timezone
from core.utils.generate_rectangles import generate_rectangles
from core.utils.generate_student_data import generate_student_data
from core.solutionA1 import SolutionA1
from core.solutionA2 import SolutionA2
from core.solutionA3 import SolutionA3
from core.solutionB1 import SolutionB1
from core.solutionB2 import SolutionB2
from core.solutionD import SolutionD
from core.solutionC import SolutionC
from core.solutionE import SolutionE

from core.xtdb_solution import XTDBSolution
from core.marklogic_solution import MarkLogicSolution
from core.xtdb_native_solution import XTDBNativeSolution

# Generate rectangles
def generate_rectangles_data(
    batch_size=20, 
    total_ids=100,  # Reduced for testing
    start_time=datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
    end_time=datetime(2025, 3, 1, 0, 0, 0, tzinfo=timezone.utc),
    num_points_per_id=50  # Reduced for testing
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

# Run solutionA async
async def run_solution(solution, rectangles):
    print(f"Starting {solution.name}...")
    await solution.insert_rectangle_to_collections(rectangles)
    print(f"{solution.name} completed")


# Main execution
async def main():
    start_time = datetime.now()
    print(f"Starting at: {start_time}")


    solutions = [
        # SolutionA1(),
        # SolutionA2(),
        # SolutionA3(), 
        # SolutionB1(),
        # SolutionB2(),
        # SolutionC(),
        # SolutionD(),
        # SolutionE(),
        # MarkLogicSolution(),
        XTDBSolution(),  # Updated XTDB solution with Postgres wire protocol
        XTDBNativeSolution()  # Native XTDB solution with JDBC transaction log
    ]
    await asyncio.gather(
        *[solution.initialize_collections() for solution in solutions]
    )

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