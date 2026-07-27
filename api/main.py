from __future__ import annotations

import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import FastAPI, Header, HTTPException, Query, Request

from api.database import (
    DEFAULT_DB_PATH,
    db_connect,
    get_delivery,
    init_source_db,
    query_deliveries,
    summary,
    utc_now,
)
from api.models import (
    DeliveryListResponse,
    DeliveryRecord,
    DeliverySummary,
    TokenRequest,
    TokenResponse,
)

CLIENT_ID = os.getenv("DELIVERY_API_CLIENT_ID", "eatngo-bi-client")
CLIENT_SECRET = os.getenv("DELIVERY_API_CLIENT_SECRET", "local-development-secret")
TOKEN_TTL_SECONDS = int(os.getenv("DELIVERY_API_TOKEN_TTL_SECONDS", "300"))
TOKENS: dict[str, float] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_source_db(
        DEFAULT_DB_PATH, reset=os.getenv("DELIVERY_API_RESET_DB", "0") == "1"
    )
    yield


app = FastAPI(
    title="Eat N' Go Delivery Partner Simulation",
    description="Independent technical case-study API simulation; not an official Eat N' Go API.",
    version="1.0.0",
    lifespan=lifespan,
)


def maybe_simulate_failure(request: Request) -> None:
    status = request.query_params.get("simulate_status")
    delay_ms = request.query_params.get("delay_ms")
    if delay_ms:
        time.sleep(int(delay_ms) / 1000)
    if status in {"429", "500", "503"}:
        headers = {"Retry-After": "1"} if status == "429" else None
        raise HTTPException(
            status_code=int(status), detail=f"Simulated HTTP {status}", headers=headers
        )


def require_token(authorization: Annotated[str | None, Header()] = None) -> None:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization.removeprefix("Bearer ").strip()
    expires_at = TOKENS.get(token)
    if not expires_at:
        raise HTTPException(status_code=401, detail="Invalid bearer token")
    if expires_at <= time.time():
        raise HTTPException(status_code=401, detail="Expired bearer token")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "name": "Eat N' Go delivery-partner API simulation",
        "official_api": False,
        "docs": "/docs",
        "health": "/health",
        "token_endpoint": "/api/v1/auth/token",
        "deliveries_endpoint": "/api/v1/deliveries",
        "summary_endpoint": "/api/v1/deliveries/summary",
    }


@app.post("/api/v1/auth/token", response_model=TokenResponse)
def token(payload: TokenRequest) -> TokenResponse:
    if payload.client_id != CLIENT_ID or payload.client_secret != CLIENT_SECRET:
        raise HTTPException(status_code=401, detail="Invalid client credentials")
    access_token = f"local-{uuid.uuid4()}"
    TOKENS[access_token] = time.time() + TOKEN_TTL_SECONDS
    return TokenResponse(access_token=access_token, expires_in=TOKEN_TTL_SECONDS)


@app.get("/api/v1/deliveries", response_model=DeliveryListResponse)
def deliveries(
    request: Request,
    cursor: str | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    updated_after: str | None = None,
    updated_before: str | None = None,
    brand: str | None = None,
    store_id: str | None = None,
    city: str | None = None,
    delivery_status: str | None = None,
) -> DeliveryListResponse:
    maybe_simulate_failure(request)
    with db_connect(DEFAULT_DB_PATH) as con:
        records, next_cursor, has_more = query_deliveries(
            con,
            cursor=cursor,
            limit=limit,
            updated_after=updated_after,
            updated_before=updated_before,
            brand=brand,
            store_id=store_id,
            city=city,
            delivery_status=delivery_status,
        )
    return DeliveryListResponse(
        data=[DeliveryRecord(**record) for record in records],
        pagination={"next_cursor": next_cursor, "has_more": has_more, "limit": limit},
        metadata={
            "request_id": str(uuid.uuid4()),
            "record_count": len(records),
            "generated_at": utc_now(),
        },
    )


@app.get("/api/v1/deliveries/summary", response_model=DeliverySummary)
def deliveries_summary(
    request: Request,
    updated_after: str | None = None,
    updated_before: str | None = None,
) -> DeliverySummary:
    maybe_simulate_failure(request)
    with db_connect(DEFAULT_DB_PATH) as con:
        return DeliverySummary(**summary(con, updated_after, updated_before))


@app.get("/api/v1/deliveries/{delivery_id}", response_model=DeliveryRecord)
def delivery_by_id(
    delivery_id: str,
) -> DeliveryRecord:
    with db_connect(DEFAULT_DB_PATH) as con:
        record = get_delivery(con, delivery_id)
    if not record:
        raise HTTPException(status_code=404, detail="Delivery not found")
    return DeliveryRecord(**record)
