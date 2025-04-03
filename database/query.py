import asyncio
from datetime import datetime, timezone
from solution1 import Solution1


async def main():
    solution1 = Solution1()
    vt = datetime(2024, 5, 17, 0, 0, 0, tzinfo=timezone.utc)
    tt = datetime(2024, 5, 18, 0, 0, 0, tzinfo=timezone.utc)

    records = await solution1.query_by_name_and_age("Student_1", 15, )
    print(records)
    
if __name__ == "__main__":
    asyncio.run(main())
