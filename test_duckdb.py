import asyncio
from core.solution3 import Solution3

async def main():
    solution3 = Solution3()
    await solution3.initialize_collections()

if __name__ == "__main__":
    asyncio.run(main())