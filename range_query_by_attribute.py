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


async def run_range_query(solution, attr_name: str, attr_value: str, vt_from: datetime, vt_to: datetime, tt_from: datetime, tt_to: datetime, verbose: bool = False):
    start_time = time.perf_counter()
    async with timer(f"{solution.name} Range Query") as t:
        records = await solution.range_query_by_attribute(attr_name, attr_value, vt_from, vt_to, tt_from, tt_to)
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
        # XTDBSolution()  # Commented out as it may not have range_query_by_attribute
    ]
    
    # Multiple input sets for testing
    input_sets = [
        {
            "attr_name": "attr4",
            "attr_value": 1949,
            "tt_from": datetime(2018, 1, 5, 0, 0, 0, tzinfo=timezone.utc),
            "tt_to": datetime(2018, 2, 6, 0, 0, 0, tzinfo=timezone.utc),
            "vt_from": datetime(2018, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
            "vt_to": datetime(2018, 2, 5, 0, 0, 0, tzinfo=timezone.utc)
        },
        {
            "attr_name": "attr1",
            "attr_value": 1500,
            "tt_from": datetime(2018, 1, 10, 0, 0, 0, tzinfo=timezone.utc),
            "tt_to": datetime(2018, 2, 10, 0, 0, 0, tzinfo=timezone.utc),
            "vt_from": datetime(2018, 1, 5, 0, 0, 0, tzinfo=timezone.utc),
            "vt_to": datetime(2018, 2, 8, 0, 0, 0, tzinfo=timezone.utc)
        },
        {
            "attr_name": "attr2",
            "attr_value": 2000,
            "tt_from": datetime(2018, 1, 15, 0, 0, 0, tzinfo=timezone.utc),
            "tt_to": datetime(2018, 2, 15, 0, 0, 0, tzinfo=timezone.utc),
            "vt_from": datetime(2018, 1, 8, 0, 0, 0, tzinfo=timezone.utc),
            "vt_to": datetime(2018, 2, 12, 0, 0, 0, tzinfo=timezone.utc)
        }
    ]

    # Collect runtime data for matrix
    runtime_matrix = []
    
    for solution in solutions:
        solution_runtimes = []
        for i, input_set in enumerate(input_sets):
            records, runtime_ms = await run_range_query(
                solution,
                input_set["attr_name"],
                input_set["attr_value"],
                input_set["vt_from"],
                input_set["vt_to"],
                input_set["tt_from"],
                input_set["tt_to"],
                verbose=False
            )
            solution_runtimes.append(f"{runtime_ms:.3f}")
        
        runtime_matrix.append([solution.name] + solution_runtimes)
    
    # Print runtime matrix table
    print("\n=== Range Query by Attribute Runtime Matrix (ms) ===")
    headers = ['Solution'] + [f'Input Set {i+1}' for i in range(len(input_sets))]
    print(tabulate(runtime_matrix, headers=headers, tablefmt='grid'))
    
    # Print input set details
    print("\n=== Input Set Details ===")
    for i, input_set in enumerate(input_sets):
        print(f"Input Set {i+1}: attr_name='{input_set['attr_name']}', attr_value={input_set['attr_value']}, tt_from={input_set['tt_from']}, tt_to={input_set['tt_to']}, vt_from={input_set['vt_from']}, vt_to={input_set['vt_to']}")
    
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