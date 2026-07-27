from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

BRANDS = ("Domino's Pizza Nigeria", "Cold Stone Creamery Nigeria", "Pinkberry Nigeria")
CITIES = ("Lagos", "Abuja", "Port Harcourt", "Ibadan", "Benin City")
STATUSES = (
    "pending",
    "assigned",
    "picked_up",
    "in_transit",
    "delivered",
    "failed",
    "cancelled",
)


class TokenRequest(BaseModel):
    client_id: str
    client_secret: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int


class DeliveryRecord(BaseModel):
    delivery_id: str
    order_id: str
    brand: str
    store_id: str
    store_name: str
    city: str
    delivery_partner: str
    delivery_status: str
    assigned_at: datetime | None = None
    picked_up_at: datetime | None = None
    promised_delivery_at: datetime | None = None
    delivered_at: datetime | None = None
    cancelled_at: datetime | None = None
    failure_reason: str | None = None
    delivery_fee: int = Field(description="Delivery fee in kobo")
    distance_km: float
    created_at: datetime
    updated_at: datetime


class Pagination(BaseModel):
    next_cursor: str | None
    has_more: bool
    limit: int


class ResponseMetadata(BaseModel):
    request_id: str
    record_count: int
    generated_at: datetime


class DeliveryListResponse(BaseModel):
    data: list[DeliveryRecord]
    pagination: Pagination
    metadata: ResponseMetadata


class DeliverySummary(BaseModel):
    source_record_count: int
    distinct_delivery_count: int
    delivered_count: int
    failed_count: int
    cancelled_count: int
    total_delivery_fees: int
    minimum_updated_at: datetime | None
    maximum_updated_at: datetime | None
    delivery_count_by_brand: dict[str, int]
    delivery_count_by_store: dict[str, int]


class QualityResult(BaseModel):
    check_name: str
    severity: str
    status: str
    failed_record_count: int
    batch_id: str
    executed_at: datetime


class ReconciliationResult(BaseModel):
    metric: str
    source_value: Any
    target_value: Any
    status: str
    batch_id: str
    executed_at: datetime
