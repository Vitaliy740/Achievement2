import os
import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    BigInteger, MetaData, Table, Column, Integer, DateTime, UniqueConstraint, select, func
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncConnection
from sqlalchemy.exc import IntegrityError

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://appuser:apppass@db:5432/numbers")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s app %(message)s")

engine: AsyncEngine = create_async_engine(DATABASE_URL, pool_pre_ping=True)

meta = MetaData()

numbers = Table(
    "numbers",
    meta,
    Column("id", Integer, primary_key=True),
    Column("value", BigInteger, nullable=False),
    Column("created_at", DateTime(timezone=False), nullable=False, default=datetime.utcnow),
    UniqueConstraint("value", name="uq_numbers_value"),
)

async def init_schema() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(meta.create_all)

async def seen(conn: AsyncConnection, n: int) -> bool:
    q = select(func.count()).select_from(numbers).where(numbers.c.value == n)
    return (await conn.execute(q)).scalar_one() > 0

async def insert_value(conn: AsyncConnection, n: int) -> None:
    await conn.execute(numbers.insert().values(value=n, created_at=datetime.utcnow()))
