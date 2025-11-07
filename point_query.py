import asyncio
import time
from datetime import datetime, timezone
from core.utils.timing import timer
from core.solutionA1 import SolutionA1
from core.solutionA2 import SolutionA2
from core.solutionB1 import SolutionB1
from core.solutionD import SolutionD
from core.solutionC import SolutionC
from core.solutionE import SolutionE
from core.xtdb_replay_solution import XTDBReplaySolution
from tabulate import tabulate
import motor.motor_asyncio


async def clear_mongodb_cache():
    """Clear MongoDB cache to ensure fair performance comparison"""
    try:
        client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://localhost:27017")
        db = client.bitemporal_db
        
        # Clear query plan cache for all collections
        collections = ["Name", "Age", "Timeslices", "Payloads"]
        for collection in collections:
            try:
                await db[collection].database.command("planCacheClear", collection)
            except Exception as e:
                # Some collections might not exist, continue with others
                pass
        
        # Clear connection pool (by closing and reopening)
        client.close()
        
        # print("MongoDB cache cleared")
    except Exception as e:
        print(f"Warning: Could not clear MongoDB cache: {e}")


async def run_query(solution, name: str, age: int, tt: datetime, vt: datetime, verbose: bool = False):
    start_time = time.perf_counter()
    async with timer(f"{solution.name} Query") as t:
        records = await solution.query_by_name_and_age(name, age, tt, vt)
        t.stage("Query Execution")

        if verbose:
            print(f"{solution.name}: Found {len(records) if records else 0} record{"s" if len(records) > 1 else ""}: ")
            print(records)
        t.stage("Results Processing")
    end_time = time.perf_counter()
    runtime_ms = (end_time - start_time) * 1000
    return records, runtime_ms

async def main():
    # Solution classes for dynamic instantiation
    solution_classes = [
        SolutionA1,
        SolutionA2,
        SolutionB1,
        SolutionC, 
        SolutionD,
        SolutionE,
        XTDBReplaySolution,
        # XTDBSolution  # Commented out as it may not have query_by_name_and_age
    ]
    
    # Multiple input sets for testing
    input_sets = [
        {
            "name": "Student_1741",
            "age": 16,
            "tt": datetime(2018, 11, 12, 0, 0, 0, tzinfo=timezone.utc),
            "vt": datetime(2018, 1, 8, 0, 0, 0, tzinfo=timezone.utc)
        },
        {
            "name": "Student_746",
            "age": 18,
            "tt": datetime(2019, 2, 11, 0, 0, 0, tzinfo=timezone.utc),
            "vt": datetime(2018, 2, 10, 0, 0, 0, tzinfo=timezone.utc)
        },
        {
            "name": "Student_3204",
            "age": 13,
            "tt": datetime(2021, 12, 9, 0, 0, 0, tzinfo=timezone.utc),
            "vt": datetime(2018, 12, 8, 0, 0, 0, tzinfo=timezone.utc)
        }
    ]

    # Collect runtime data for matrix
    runtime_matrix = []
    
    for solution_class in solution_classes:
        solution_runtimes = []
        for i, input_set in enumerate(input_sets):
            # Create a new solution instance for each query to ensure fairness
            solution = solution_class()
            
            # Clear MongoDB cache before each query
            await clear_mongodb_cache()
            
            records, runtime_ms = await run_query(
                solution, 
                input_set["name"], 
                input_set["age"], 
                input_set["tt"], 
                input_set["vt"],
                verbose=False
            )
            print(records)
            solution_runtimes.append(f"{runtime_ms:.3f}")
            
            # Clean up solution instance if it has a close method
            if hasattr(solution, 'close'):
                await solution.close()
        
        runtime_matrix.append([solution_class().name] + solution_runtimes)
    
    # Print runtime matrix table
    print("\n=== Point Query Runtime Matrix (ms) ===")
    headers = ['Solution'] + [f'Input Set {i+1}' for i in range(len(input_sets))]
    print(tabulate(runtime_matrix, headers=headers, tablefmt='grid'))
    
    # Print input set details
    print("\n=== Input Set Details ===")
    for i, input_set in enumerate(input_sets):
        print(f"Input Set {i+1}: name='{input_set['name']}', age={input_set['age']}, tt={input_set['tt']}, vt={input_set['vt']}")
    
    # Print statistics for each input set
    print("\n=== Statistics ===")
    for i in range(len(input_sets)):
        runtimes = [float(row[i+1]) for row in runtime_matrix]
        print(f"Input Set {i+1} - Fastest: {min(runtimes):.3f}ms, Slowest: {max(runtimes):.3f}ms, Average: {sum(runtimes)/len(runtimes):.3f}ms")
    
    # Overall statistics
    all_runtimes = [float(runtime) for row in runtime_matrix for runtime in row[1:]]
    print(f"Overall - Fastest: {min(all_runtimes):.3f}ms, Slowest: {max(all_runtimes):.3f}ms, Average: {sum(all_runtimes)/len(all_runtimes):.3f}ms")
if __name__ == "__main__":
    asyncio.run(main())