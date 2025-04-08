import asyncio
from datetime import datetime, timezone
import importlib
from typing import List, Type
from utils.timing import timer

async def run_query(solution_class: Type, name: str, age: int, tt: datetime, vt: datetime):
    async with timer(f"{solution_class.__name__} Query") as t:
        solution = solution_class()
        t.stage("Initialization")

        records = await solution.query_by_name_and_age(name, age, tt, vt)
        print(records)
        t.stage("Query Execution")

        print(f"{solution_class.__name__}: Found {len(records) if records else 0} records")
        t.stage("Results Processing")
        return records

async def main():
    # Dynamic solution loading
    solutions = []
    for i in [1, 2]:  # Adjust range based on number of solutions
        module = importlib.import_module(f"core.solution{i}")
        solution_class = getattr(module, f"Solution{i}")
        solutions.append(solution_class)

    # Query parameters
    tt = datetime(2018, 12, 13, 0, 0, 0, tzinfo=timezone.utc)
    vt = datetime(2018, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
    name, age = "Student_1", 17

    # Run all solutions concurrently
    async with timer("Total Execution") as t:
        tasks = [run_query(solution, name, age, tt, vt) for solution in solutions]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())