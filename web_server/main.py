from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel, Field, ValidationError, ConfigDict
import httpx
import logging
import os
from uuid import uuid4

APP_SERVER_URL = os.getenv("APP_SERVER_URL", "http://localhost:8001")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s web %(message)s",
)

app = FastAPI(
    title="Web Gateway",
    version="1.0.0",
    description="Тонкий шлюз к серверу приложений. POST /api/v1/increment возвращает n+1.",
)

class InRequest(BaseModel):
    value: int = Field(..., ge=0, description="Натуральное число N≥0")
    # пример для Swagger
    model_config = ConfigDict(json_schema_extra={"example": {"value": 10}})

class OutResponse(BaseModel):
    result: int = Field(..., description="n+1")

class ErrorBody(BaseModel):
    code: str
    message: str

class ErrorResponse(BaseModel):
    error: ErrorBody


@app.post(
    "/api/v1/increment",
    response_model=OutResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Неверный JSON/схема или выход за диапазон"},
        409: {"model": ErrorResponse, "description": "DUPLICATE или PREDECESSOR_OF_EXISTING"},
        422: {"description": "Валидация тела запроса (FastAPI)"},
        502: {"model": ErrorResponse, "description": "Сервер приложений недоступен"},
    },
    tags=["increment"],
)
async def increment(body: InRequest, req: Request):
    """
    Валидирует вход (через Pydantic) и проксирует на сервер приложений.
    Теперь Swagger покажет тело запроса.
    """
    rid = req.headers.get("X-Request-ID") or str(uuid4())

    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            r = await client.post(
                f"{APP_SERVER_URL}/process",
                json={"value": body.value},
                headers={"X-Request-ID": rid},
            )
        except httpx.RequestError as e:
            logging.error("upstream_unavailable rid=%s err=%s", rid, e)
            raise HTTPException(
                status_code=502,
                detail={"error": {"code": "UPSTREAM_UNAVAILABLE", "message": "application server unavailable"}},
            )

    if r.status_code == 200:
        logging.info("ok rid=%s n=%s", rid, body.value)
        return r.json()

    detail = r.json()
    logging.warning("app_error rid=%s n=%s resp=%s", rid, body.value, detail)
    raise HTTPException(status_code=r.status_code, detail=detail)

