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
        # XTDBSolution()  # Commented out as it may not have query_by_name_and_age
    ]
    
    # Multiple input sets for testing
    input_sets = [
        {
            "name": "Student_2519",
            "age": 11,
            "tt": datetime(2018, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
            "vt": datetime(2018, 1, 5, 0, 0, 0, tzinfo=timezone.utc)
        },
        {
            "name": "Student_1000",
            "age": 15,
            "tt": datetime(2018, 1, 10, 0, 0, 0, tzinfo=timezone.utc),
            "vt": datetime(2018, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
        },
        {
            "name": "Student_500",
            "age": 20,
            "tt": datetime(2018, 2, 1, 0, 0, 0, tzinfo=timezone.utc),
            "vt": datetime(2018, 2, 5, 0, 0, 0, tzinfo=timezone.utc)
        }
    ]

    # Collect runtime data for matrix
    runtime_matrix = []
    
    for solution in solutions:
        solution_runtimes = []
        for i, input_set in enumerate(input_sets):
            records, runtime_ms = await run_query(
                solution, 
                input_set["name"], 
                input_set["age"], 
                input_set["tt"], 
                input_set["vt"],
                verbose=False
            )
            solution_runtimes.append(f"{runtime_ms:.3f}")
        
        runtime_matrix.append([solution.name] + solution_runtimes)
    
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