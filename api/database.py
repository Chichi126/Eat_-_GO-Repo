from __future__ import annotations

import base64
import json
import os
import random
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from faker import Faker

from api.models import BRANDS, CITIES

DEFAULT_DB_PATH = Path(
    os.getenv("DELIVERY_API_DB_PATH", "/tmp/eatngo_delivery_partner_api.sqlite")
)
SOURCE_SEED = 20260725
STORE_COUNT = 15
BASE_RECORD_COUNT = 1_000
OLD_BRAND_TO_OFFICIAL = {
    "Domino's Pizza": "Domino's Pizza Nigeria",
    "Cold Stone Creamery": "Cold Stone Creamery Nigeria",
    "Pinkberry": "Pinkberry Nigeria",
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def to_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return (
        value.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def parse_dt(value: str | None) -> datetime | None:
    if value is None:
        return None
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def db_connect(path: Path | str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con


def source_schema_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS deliveries (
        source_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
        delivery_id TEXT,
        order_id TEXT,
        brand TEXT,
        store_id TEXT,
        store_name TEXT,
        city TEXT,
        delivery_partner TEXT,
        delivery_status TEXT,
        assigned_at TEXT,
        picked_up_at TEXT,
        promised_delivery_at TEXT,
        delivered_at TEXT,
        cancelled_at TEXT,
        failure_reason TEXT,
        delivery_fee INTEGER,
        distance_km REAL,
        created_at TEXT,
        updated_at TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_deliveries_updated_id ON deliveries(updated_at, delivery_id);
    CREATE INDEX IF NOT EXISTS idx_deliveries_filters ON deliveries(brand, store_id, city, delivery_status);
    """


def create_source_schema(con: sqlite3.Connection) -> None:
    con.executescript(source_schema_sql())


def migrate_brand_values(con: sqlite3.Connection) -> None:
    """Correct legacy simulated brand values without changing store names."""
    for old_brand, official_brand in OLD_BRAND_TO_OFFICIAL.items():
        con.execute(
            "UPDATE deliveries SET brand = ? WHERE brand = ?",
            [official_brand, old_brand],
        )
    con.commit()


def insert_delivery(con: sqlite3.Connection, record: dict[str, Any]) -> None:
    columns = [
        "delivery_id",
        "order_id",
        "brand",
        "store_id",
        "store_name",
        "city",
        "delivery_partner",
        "delivery_status",
        "assigned_at",
        "picked_up_at",
        "promised_delivery_at",
        "delivered_at",
        "cancelled_at",
        "failure_reason",
        "delivery_fee",
        "distance_km",
        "created_at",
        "updated_at",
    ]
    placeholders = ", ".join(["?"] * len(columns))
    con.execute(
        f"INSERT INTO deliveries ({', '.join(columns)}) VALUES ({placeholders})",
        [record.get(column) for column in columns],
    )


def store_catalog() -> list[dict[str, str]]:
    stores = []
    for idx in range(1, STORE_COUNT + 1):
        brand = BRANDS[(idx - 1) % len(BRANDS)]
        city = CITIES[(idx - 1) % len(CITIES)]
        stores.append(
            {
                "store_id": f"S{idx:03d}",
                "store_name": f"ENG {brand.removesuffix(' Nigeria')} {idx}",
                "brand": brand,
                "city": city,
            }
        )
    return stores


def build_record(
    fake: Faker,
    rng: random.Random,
    idx: int,
    store: dict[str, str],
    created_at: datetime,
) -> dict[str, Any]:
    status_weights = (
        ["delivered"] * 70
        + ["failed"] * 8
        + ["cancelled"] * 7
        + ["in_transit"] * 6
        + ["picked_up"] * 4
        + ["assigned"] * 3
        + ["pending"] * 2
    )
    status = rng.choice(status_weights)
    assigned_at = created_at + timedelta(minutes=rng.randint(1, 12))
    picked_up_at = (
        assigned_at + timedelta(minutes=rng.randint(5, 20))
        if status in {"picked_up", "in_transit", "delivered", "failed"}
        else None
    )
    promised_delivery_at = assigned_at + timedelta(
        minutes=rng.choice([30, 35, 40, 45, 50])
    )
    delivered_at = None
    cancelled_at = None
    failure_reason = None
    if status == "delivered":
        delay = rng.choice([-8, -4, 0, 5, 12, 22])
        delivered_at = promised_delivery_at + timedelta(minutes=delay)
    elif status == "failed":
        failure_reason = rng.choice(
            ["customer_unavailable", "rider_breakdown", "address_not_found"]
        )
    elif status == "cancelled":
        cancelled_at = assigned_at + timedelta(minutes=rng.randint(2, 18))

    updated_candidates = [created_at, assigned_at, promised_delivery_at]
    for item in [picked_up_at, delivered_at, cancelled_at]:
        if item is not None:
            updated_candidates.append(item)
    updated_at = max(updated_candidates) + timedelta(minutes=rng.randint(0, 90))

    return {
        "delivery_id": f"D{idx:07d}",
        "order_id": f"O{idx:07d}",
        "brand": store["brand"],
        "store_id": store["store_id"],
        "store_name": store["store_name"],
        "city": store["city"],
        "delivery_partner": fake.company(),
        "delivery_status": status,
        "assigned_at": to_iso(assigned_at),
        "picked_up_at": to_iso(picked_up_at),
        "promised_delivery_at": to_iso(promised_delivery_at),
        "delivered_at": to_iso(delivered_at),
        "cancelled_at": to_iso(cancelled_at),
        "failure_reason": failure_reason,
        "delivery_fee": rng.choice([50000, 70000, 90000, 120000, 150000]),
        "distance_km": round(rng.uniform(0.8, 18.0), 2),
        "created_at": to_iso(created_at),
        "updated_at": to_iso(updated_at),
    }


def seed_source_data(con: sqlite3.Connection, include_invalid: bool = False) -> None:
    existing = con.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0]
    if existing:
        return

    fake = Faker("en_NG")
    fake.seed_instance(SOURCE_SEED)
    rng = random.Random(SOURCE_SEED)
    stores = store_catalog()
    start = datetime(2026, 5, 1, tzinfo=timezone.utc)

    records: list[dict[str, Any]] = []
    for idx in range(1, BASE_RECORD_COUNT + 1):
        store = stores[idx % len(stores)]
        created_at = start + timedelta(minutes=idx * 90)
        records.append(build_record(fake, rng, idx, store, created_at))

    for record in records:
        insert_delivery(con, record)

    # Duplicate source rows, representing provider replay.
    for record in records[:10]:
        insert_delivery(con, record)

    # Corrections with the same delivery_id and later updated_at.
    for record in records[10:30]:
        corrected = dict(record)
        corrected["delivery_status"] = "delivered"
        picked_up = parse_dt(corrected["picked_up_at"]) or parse_dt(
            corrected["assigned_at"]
        ) + timedelta(minutes=10)
        corrected["picked_up_at"] = to_iso(picked_up)
        corrected["delivered_at"] = to_iso(picked_up + timedelta(minutes=25))
        corrected["cancelled_at"] = None
        corrected["failure_reason"] = None
        corrected["delivery_fee"] = int(corrected["delivery_fee"]) + 5000
        corrected["updated_at"] = to_iso(
            parse_dt(corrected["updated_at"]) + timedelta(days=2)
        )
        insert_delivery(con, corrected)

    # Late-arriving rows: old created_at, recent updated_at.
    for record in records[30:45]:
        late = dict(record)
        late["updated_at"] = to_iso(parse_dt(late["created_at"]) + timedelta(days=12))
        insert_delivery(con, late)

    if include_invalid:
        bad = dict(records[0])
        bad["delivery_id"] = None
        bad["delivery_fee"] = -100
        bad["distance_km"] = -1
        bad["brand"] = "Unexpected Brand"
        bad["updated_at"] = to_iso(utc_now() + timedelta(days=1))
        insert_delivery(con, bad)

    con.commit()


def init_source_db(
    path: Path | str = DEFAULT_DB_PATH,
    reset: bool = False,
    include_invalid: bool = False,
) -> Path:
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if reset and db_path.exists():
        db_path.unlink()
    con = db_connect(db_path)
    try:
        create_source_schema(con)
        migrate_brand_values(con)
        seed_source_data(con, include_invalid=include_invalid)
        migrate_brand_values(con)
    finally:
        con.close()
    return db_path


def encode_cursor(offset: int) -> str:
    return base64.urlsafe_b64encode(
        json.dumps({"offset": offset}).encode("utf-8")
    ).decode("ascii")


def decode_cursor(cursor: str | None) -> int:
    if not cursor:
        return 0
    try:
        payload = json.loads(
            base64.urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8")
        )
        return int(payload.get("offset", 0))
    except Exception as exc:
        raise ValueError("Invalid cursor") from exc


def row_to_delivery(row: sqlite3.Row) -> dict[str, Any]:
    columns = row.keys()
    return {key: row[key] for key in columns if key != "source_row_id"}


def filter_clause(
    updated_after: str | None,
    updated_before: str | None,
    brand: str | None,
    store_id: str | None,
    city: str | None,
    delivery_status: str | None,
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if updated_after:
        clauses.append("updated_at > ?")
        params.append(updated_after)
    if updated_before:
        clauses.append("updated_at <= ?")
        params.append(updated_before)
    if brand:
        clauses.append("brand = ?")
        params.append(brand)
    if store_id:
        clauses.append("store_id = ?")
        params.append(store_id)
    if city:
        clauses.append("city = ?")
        params.append(city)
    if delivery_status:
        clauses.append("delivery_status = ?")
        params.append(delivery_status)
    where_sql = "WHERE " + " AND ".join(clauses) if clauses else ""
    return where_sql, params


def query_deliveries(
    con: sqlite3.Connection,
    *,
    cursor: str | None,
    limit: int,
    updated_after: str | None = None,
    updated_before: str | None = None,
    brand: str | None = None,
    store_id: str | None = None,
    city: str | None = None,
    delivery_status: str | None = None,
) -> tuple[list[dict[str, Any]], str | None, bool]:
    offset = decode_cursor(cursor)
    where_sql, params = filter_clause(
        updated_after, updated_before, brand, store_id, city, delivery_status
    )
    rows = con.execute(
        f"""
        SELECT *
        FROM deliveries
        {where_sql}
        ORDER BY updated_at, delivery_id
        LIMIT ? OFFSET ?
        """,
        [*params, limit + 1, offset],
    ).fetchall()
    page_rows = rows[:limit]
    has_more = len(rows) > limit
    next_cursor = encode_cursor(offset + limit) if has_more else None
    return [row_to_delivery(row) for row in page_rows], next_cursor, has_more


def get_delivery(con: sqlite3.Connection, delivery_id: str) -> dict[str, Any] | None:
    row = con.execute(
        """
        SELECT *
        FROM deliveries
        WHERE delivery_id = ?
        ORDER BY updated_at DESC, source_row_id DESC
        LIMIT 1
        """,
        [delivery_id],
    ).fetchone()
    return row_to_delivery(row) if row else None


def summary(
    con: sqlite3.Connection, updated_after: str | None, updated_before: str | None
) -> dict[str, Any]:
    where_sql, params = filter_clause(
        updated_after, updated_before, None, None, None, None
    )
    source = con.execute(
        f"""
        SELECT
            COUNT(*) AS source_record_count,
            COUNT(DISTINCT delivery_id) AS distinct_delivery_count,
            MIN(updated_at) AS minimum_updated_at,
            MAX(updated_at) AS maximum_updated_at
        FROM deliveries
        {where_sql}
        """,
        params,
    ).fetchone()
    latest = con.execute(
        f"""
        WITH ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY delivery_id ORDER BY updated_at DESC, source_row_id DESC) AS rn
            FROM deliveries
            {where_sql}
        )
        SELECT
            SUM(CASE WHEN delivery_status = 'delivered' THEN 1 ELSE 0 END) AS delivered_count,
            SUM(CASE WHEN delivery_status = 'failed' THEN 1 ELSE 0 END) AS failed_count,
            SUM(CASE WHEN delivery_status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count,
            COALESCE(SUM(delivery_fee), 0) AS total_delivery_fees
        FROM ranked
        WHERE rn = 1
        """,
        params,
    ).fetchone()
    brand_rows = con.execute(
        f"""
        WITH ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY delivery_id ORDER BY updated_at DESC, source_row_id DESC) AS rn
            FROM deliveries
            {where_sql}
        )
        SELECT brand, COUNT(*) AS count
        FROM ranked
        WHERE rn = 1
        GROUP BY brand
        ORDER BY brand
        """,
        params,
    ).fetchall()
    store_rows = con.execute(
        f"""
        WITH ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY delivery_id ORDER BY updated_at DESC, source_row_id DESC) AS rn
            FROM deliveries
            {where_sql}
        )
        SELECT store_id, COUNT(*) AS count
        FROM ranked
        WHERE rn = 1
        GROUP BY store_id
        ORDER BY store_id
        """,
        params,
    ).fetchall()
    return {
        "source_record_count": source["source_record_count"] or 0,
        "distinct_delivery_count": source["distinct_delivery_count"] or 0,
        "delivered_count": latest["delivered_count"] or 0,
        "failed_count": latest["failed_count"] or 0,
        "cancelled_count": latest["cancelled_count"] or 0,
        "total_delivery_fees": latest["total_delivery_fees"] or 0,
        "minimum_updated_at": source["minimum_updated_at"],
        "maximum_updated_at": source["maximum_updated_at"],
        "delivery_count_by_brand": {row["brand"]: row["count"] for row in brand_rows},
        "delivery_count_by_store": {
            row["store_id"]: row["count"] for row in store_rows
        },
    }
