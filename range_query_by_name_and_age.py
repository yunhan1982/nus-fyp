import asyncio
import time
from datetime import datetime, timezone
from core.utils.timing import timer
from core.solutionA1 import SolutionA1
from core.solutionA2 import SolutionA2
from core.solutionA3 import SolutionA3
from core.solutionB1 import SolutionB1
from core.solutionB2 import SolutionB2
from core.solutionD import SolutionD
from core.solutionC import SolutionC
from core.solutionE import SolutionE
from core.xtdb_solution import XTDBSolution
from tabulate import tabulate


async def run_range_query(solution, name: str, age: int, vt_from: datetime, vt_to: datetime, tt_from: datetime, tt_to: datetime, verbose: bool = False):
    start_time = time.perf_counter()
    async with timer(f"{solution.name} Range Query") as t:
        records = await solution.range_query_by_name_and_age(name, age, vt_from, vt_to, tt_from, tt_to)
        t.stage("Query Execution")

        if verbose:
            print(f"{solution.name}: Found {len(records) if records else 0} record{"s" if len(records) > 1 else ""}: ")
            print(records)
        t.stage("Results Processing")
    end_time = time.perf_counter()
    runtime_ms = (end_time - start_time) * 1000
    return records, runtime_ms

async def main():
    # Dynamic solution loading
    solutions = [
        SolutionA1(),
        SolutionA2(),
        SolutionA3(), 
        SolutionB1(),
        SolutionB2(),
        SolutionC(), 
        SolutionD(),
        SolutionE(),
        # XTDBSolution()  # Commented out as it may not have range_query_by_name_and_age
    ]
    
    # List of input sets - each tuple contains (name, age, vt_from, vt_to, tt_from, tt_to)
    input_sets = [
        # Input Set 1
        ("Student_1669", 12, 
         datetime(2018, 2, 20, 0, 0, 0, tzinfo=timezone.utc),  # vt_from
         datetime(2018, 3, 15, 0, 0, 0, tzinfo=timezone.utc),  # vt_to
         datetime(2018, 2, 17, 0, 0, 0, tzinfo=timezone.utc),  # tt_from
         datetime(2018, 2, 23, 0, 0, 0, tzinfo=timezone.utc)), # tt_to
        
        # Input Set 2
        ("Student_500", 15,
         datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc),   # vt_from
         datetime(2018, 6, 30, 0, 0, 0, tzinfo=timezone.utc),  # vt_to
         datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc),   # tt_from
         datetime(2018, 12, 31, 0, 0, 0, tzinfo=timezone.utc)), # tt_to
        
        # Input Set 3
        ("Student_1000", 18,
         datetime(2018, 3, 1, 0, 0, 0, tzinfo=timezone.utc),   # vt_from
         datetime(2018, 4, 30, 0, 0, 0, tzinfo=timezone.utc),  # vt_to
         datetime(2018, 3, 1, 0, 0, 0, tzinfo=timezone.utc),   # tt_from
         datetime(2018, 4, 30, 0, 0, 0, tzinfo=timezone.utc)), # tt_to
    ]

    # Collect runtime data for each solution across all input sets
    runtime_matrix = []
    
    for solution in solutions:
        solution_runtimes = [solution.name]  # Start with solution name
        
        for i, (name, age, vt_from, vt_to, tt_from, tt_to) in enumerate(input_sets):
            print(f"Running {solution.name} with Input Set {i+1}...")
            records, runtime_ms = await run_range_query(solution, name, age, vt_from, vt_to, tt_from, tt_to, verbose=False)
            solution_runtimes.append(f"{runtime_ms:.3f}")
            print(f"  Found {len(records) if records else 0} records in {runtime_ms:.3f}ms")
        
        runtime_matrix.append(solution_runtimes)
    
    # Create headers for the table
    headers = ['Solution'] + [f'Input Set {i+1} (ms)' for i in range(len(input_sets))]
    
    # Print runtime matrix table
    print("\n=== Runtime Matrix (ms) ===")
    print(tabulate(runtime_matrix, headers=headers, tablefmt='grid'))
    
    # Print input set details
    print("\n=== Input Set Details ===")
    for i, (name, age, vt_from, vt_to, tt_from, tt_to) in enumerate(input_sets):
        print(f"Input Set {i+1}: name='{name}', age={age}")
        print(f"  Valid Time: {vt_from.strftime('%Y-%m-%d')} to {vt_to.strftime('%Y-%m-%d')}")
        print(f"  Transaction Time: {tt_from.strftime('%Y-%m-%d')} to {tt_to.strftime('%Y-%m-%d')}")
    
    # Print statistics for each input set
    print("\n=== Statistics by Input Set ===")
    for i in range(len(input_sets)):
        runtimes = [float(row[i+1]) for row in runtime_matrix]  # Skip solution name column
        print(f"Input Set {i+1}:")
        print(f"  Fastest: {min(runtimes):.3f}ms")
        print(f"  Slowest: {max(runtimes):.3f}ms")
        print(f"  Average: {sum(runtimes)/len(runtimes):.3f}ms")
    
    # Print overall statistics
    all_runtimes = []
    for row in runtime_matrix:
        all_runtimes.extend([float(runtime) for runtime in row[1:]])  # Skip solution name
    
    print("\n=== Overall Statistics ===")
    print(f"Fastest: {min(all_runtimes):.3f}ms")
    print(f"Slowest: {max(all_runtimes):.3f}ms")
    print(f"Average: {sum(all_runtimes)/len(all_runtimes):.3f}ms")
if __name__ == "__main__":
    asyncio.run(main())