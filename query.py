import asyncio
from datetime import datetime, timezone
from utils.timing import timer
from core.solution1 import Solution1
from core.solution2 import Solution2
from core.solution3 import Solution3

async def run_query(solution, name: str, age: int, tt: datetime, vt: datetime):
    async with timer(f"{solution.name} Query") as t:
        records = await solution.query_by_name_and_age(name, age, tt, vt)
        t.stage("Query Execution")

        print(f"{solution.name}: Found {len(records) if records else 0} record: ")
        print(records)
        t.stage("Results Processing")
        return records

async def main():
    # Dynamic solution loading
    solutions = [
        Solution1(), 
        Solution2(), 
        Solution3()
    ]

    # Query parameters
    tt = datetime(2018, 9, 1, 0, 0, 0, tzinfo=timezone.utc)
    vt = datetime(2018, 2, 5, 0, 0, 0, tzinfo=timezone.utc)
    name, age = "Student_2492", 11

    async with timer("Total Execution") as t:
        for solution in solutions:
            await run_query(solution, name, age, tt, vt) 
if __name__ == "__main__":
    asyncio.run(main())