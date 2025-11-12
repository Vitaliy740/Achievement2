import os
import logging
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.exc import IntegrityError
from sqlalchemy import select
from .db import engine, init_schema, seen, insert_value, numbers

MAX_N = int(os.getenv("MAX_N", str(2**63 - 1)))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s app %(message)s")
logger = logging.getLogger("app")

app = FastAPI(
    title="Application Server",
    version="1.0.0",
    description="Сервис бизнес-логики. Принимает n и возвращает n+1. Обрабатывает DUPLICATE и PREDECESSOR_OF_EXISTING.",
)

class InRequest(BaseModel):
    value: int = Field(..., ge=0, description="Натуральное число N≥0")

class OutResponse(BaseModel):
    result: int = Field(..., description="n+1")

class ErrorBody(BaseModel):
    code: str
    message: str

class ErrorResponse(BaseModel):
    error: ErrorBody

@app.on_event("startup")
async def _startup():
    await init_schema()

@app.post(
    "/process",
    response_model=OutResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Неверный диапазон или формат"},
        409: {"model": ErrorResponse, "description": "DUPLICATE или PREDECESSOR_OF_EXISTING"},
    },
    tags=["increment"],
)
async def process(req: Request, body: InRequest):
    """
    Правила:
    - Успех: вернуть value+1.
    - E1: если value уже в БД -> 409 DUPLICATE.
    - E2: если (value+1) уже в БД -> 409 PREDECESSOR_OF_EXISTING.
    """
    rid = req.headers.get("X-Request-ID", "-")
    n = body.value
    if n > MAX_N:
        err = {"code": "OUT_OF_RANGE", "message": f"value > MAX_N ({MAX_N})"}
        logger.warning("rid=%s n=%s %s", rid, n, err)
        raise HTTPException(status_code=400, detail={"error": err})

    async with engine.begin() as conn:  # type: AsyncConnection
        if await seen(conn, n):
            err = {"code": "DUPLICATE", "message": "number already processed"}
            logger.error("rid=%s n=%s %s", rid, n, err)
            raise HTTPException(status_code=409, detail={"error": err})

        if await seen(conn, n + 1):
            err = {"code": "PREDECESSOR_OF_EXISTING", "message": "n is one less than an already processed number"}
            logger.error("rid=%s n=%s %s", rid, n, err)
            raise HTTPException(status_code=409, detail={"error": err})

        try:
            await insert_value(conn, n)
        except IntegrityError:
            err = {"code": "DUPLICATE", "message": "number already processed"}
            logger.error("rid=%s n=%s %s", rid, n, err)
            raise HTTPException(status_code=409, detail={"error": err})

    return {"result": n + 1}
