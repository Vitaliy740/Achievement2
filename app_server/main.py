import os
import logging
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.exc import IntegrityError
from sqlalchemy import select
from db import engine, init_schema, seen, insert_value, numbers


MAX_N = int(os.getenv("MAX_N", str(2**63 - 1)))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s app %(message)s")
logger = logging.getLogger("app")

app = FastAPI(title="Application Server")

class InRequest(BaseModel):
    value: int = Field(..., ge=0)

@app.on_event("startup")
async def _startup():
    await init_schema()

@app.post("/process")
async def process(req: Request, body: InRequest):
    """
    Правила:
    - Успех: вернуть value+1.
    - E1: если value уже в БД -> 409 DUPLICATE.
    - E2: если (value+1) уже в БД -> 409 PREDECESSOR_OF_EXISTING.
    Вся логика внутри транзакции.
    """
    rid = req.headers.get("X-Request-ID", "-")
    n = body.value
    if n > MAX_N:
        err = {"code": "OUT_OF_RANGE", "message": f"value > MAX_N ({MAX_N})"}
        logger.warning("rid=%s n=%s %s", rid, n, err)
        raise HTTPException(status_code=400, detail={"error": err})

    async with engine.begin() as conn:  # type: AsyncConnection
        # Проверка E1
        if await seen(conn, n):
            err = {"code": "DUPLICATE", "message": "number already processed"}
            logger.error("rid=%s n=%s %s", rid, n, err)
            raise HTTPException(status_code=409, detail={"error": err})

        # Проверка E2
        if await seen(conn, n + 1):
            err = {"code": "PREDECESSOR_OF_EXISTING", "message": "n is one less than an already processed number"}
            logger.error("rid=%s n=%s %s", rid, n, err)
            raise HTTPException(status_code=409, detail={"error": err})

        # Вставка. При гонке дубликатов поймаем IntegrityError и вернём E1.
        try:
            await insert_value(conn, n)
        except IntegrityError:
            err = {"code": "DUPLICATE", "message": "number already processed"}
            logger.error("rid=%s n=%s %s", rid, n, err)
            raise HTTPException(status_code=409, detail={"error": err})

    res = {"result": n + 1}
    logger.info("rid=%s n=%s ok result=%s", rid, n, res["result"])
    return res
