import asyncio
from datetime import datetime, timezone
from core.utils.timing import timer
from core.utils.timing import timer
from core.solution1 import Solution1
from core.solution2 import Solution2
from core.solution3 import Solution3
from core.solution4 import Solution4
from core.solution5 import Solution5
from core.xtdb_solution import XTDBSolution

async def run_query(solution, name: str, age: int, tt: datetime, vt: datetime):
    async with timer(f"{solution.name} Query") as t:
        records = await solution.query_by_name_and_age(name, age, tt, vt)
        t.stage("Query Execution")

        print(f"{solution.name}: Found {len(records) if records else 0} record{"s" if len(records) > 1 else ""}: ")
        print(records)
        t.stage("Results Processing")
        return records

async def main():
    # Dynamic solution loading
    solutions = [
        # Solution1(), 
        Solution2(), 
        Solution3(),
        Solution4(),
        Solution5(),
        XTDBSolution()
    ]
    # Query parameters
    tt = datetime(2025, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    vt = datetime(2025, 1, 5, 0, 0, 0, tzinfo=timezone.utc)
    name, age = "Student_3932", 16


    for solution in solutions:
        await run_query(solution, name, age, tt, vt)
if __name__ == "__main__":
    asyncio.run(main())