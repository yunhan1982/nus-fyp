import asyncio
import motor.motor_asyncio
from datetime import datetime, timezone
from generate_rectangles import generate_rectangles
from generate_student_data import generate_student_data

# Generate rectangles
def generate_rectangles_data():
    return generate_rectangles(
        start_time=datetime(2024, 5, 16, 0, 0, 0, tzinfo=timezone.utc),
        end_time=datetime(2024, 5, 20, 0, 0, 0, tzinfo=timezone.utc),
        num_ids=100,
        num_points_per_id=100,
        generate_data=generate_student_data
    )

# Run solution1 async
async def run_solution1(rectangles):
    client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017/', uuidRepresentation='standard')
    try:
        from solution1 import insert_rectangle_to_collections
        db = client["Solution1"]
        print("Starting Solution 1...")
        await insert_rectangle_to_collections(rectangles, db)
        print("Solution 1 completed")
    finally:
        client.close()

# Run solution2 async
async def run_solution2(rectangles):
    client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017/', uuidRepresentation='standard')
    try:
        from solution2 import insert_rectangle_to_collections
        db = client["Solution2"]
        print("Starting Solution 2...")
        await insert_rectangle_to_collections(rectangles, db)
        print("Solution 2 completed")
    finally:
        client.close()

# Run solution3 async
async def run_solution3(rectangles):
    client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017/', uuidRepresentation='standard')
    try:
        from solution3 import insert_rectangle_to_collections
        db = client["Solution3"]
        print("Starting Solution 3...")
        await insert_rectangle_to_collections(rectangles, db)
        print("Solution 3 completed")
    finally:
        client.close()

# Main execution
async def main():
    start_time = datetime.now()
    print(f"Starting at: {start_time}")
    
    rectangles = generate_rectangles_data()
    print(f"Generated {len(rectangles)} rectangles")
    
    # Run all solutions concurrently
    await asyncio.gather(
        run_solution1(rectangles),
        run_solution2(rectangles),
        run_solution3(rectangles)
    )
    
    end_time = datetime.now()
    print(f"All solutions executed successfully")
    print(f"Total execution time: {end_time - start_time}")

if __name__ == "__main__":
    asyncio.run(main())