import asyncio
from db import engine
from models import Base

async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("All tables created.")

if __name__ == "__main__":
    asyncio.run(create_tables())
