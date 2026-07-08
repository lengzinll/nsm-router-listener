from fastapi import FastAPI, Query
from typing import Optional
from .db import DATABASE_URL
import asyncpg
from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    db_pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=2,
        max_size=10
    )
    print("Database pool started")
    yield
    if db_pool:
        await db_pool.close()
    print("Database pool closed")

app = FastAPI(
    title="Router Logs API",
    lifespan=lifespan,
)



from typing import Optional
from fastapi import Query, HTTPException

ALLOWED_SORT_COLUMNS = {
    "id",
    "source_device_ip",
    "source_ip",
    "destination_ip",
    "source_port",
    "destination_port",
    "protocol",
    "action",
    "router",
    "connection_state",
    "priority",
    "created_at",
}


@app.get("/logs", tags=["Router logs"])
async def get_logs(
    # filters
    source_device_ip: Optional[str] = None,
    source_ip: Optional[str] = None,
    destination_ip: Optional[str] = None,
    source_port: Optional[int] = None,
    destination_port: Optional[int] = None,
    protocol: Optional[str] = None,
    action: Optional[str] = None,
    router: Optional[str] = None,
    connection_state: Optional[str] = None,
    priority: Optional[int] = None,

    # sort
    sort_by: str = Query("id", description=f"One of: {', '.join(sorted(ALLOWED_SORT_COLUMNS))}"),
    sort_order: str = Query("desc", pattern="^(?i)(asc|desc)$"),

    # pagination
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=10000)
):

    # Validate sort column against whitelist to prevent SQL injection
    if sort_by not in ALLOWED_SORT_COLUMNS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid sort_by column '{sort_by}'. Allowed: {', '.join(sorted(ALLOWED_SORT_COLUMNS))}"
        )

    order_direction = "ASC" if sort_order.lower() == "asc" else "DESC"

    # Add id as a secondary tiebreaker for stable pagination when sort_by isn't unique
    if sort_by == "id":
        order_clause = f"ORDER BY id {order_direction}"
    else:
        order_clause = f"ORDER BY {sort_by} {order_direction}, id {order_direction}"

    filters = []
    values = []
    index = 1

    def add_filter(sql, value):
        nonlocal index
        if value is not None:
            filters.append(sql.format(index))
            values.append(value)
            index += 1

    add_filter("source_device_ip = ${}",source_device_ip)
    add_filter("source_ip = ${}",source_ip)
    add_filter("destination_ip = ${}",destination_ip)
    add_filter("source_port = ${}",source_port)
    add_filter("destination_port = ${}",destination_port)
    add_filter("protocol = ${}",protocol)
    add_filter("action = ${}",action)
    add_filter("router = ${}",router)
    add_filter("connection_state = ${}",connection_state)
    add_filter("priority = ${}",priority)
    
    where = ""

    if filters:
        where = "WHERE " + " AND ".join(filters)

    offset = (page - 1) * page_size
    async with db_pool.acquire() as conn:
        total = await conn.fetchval(
            f"""
            SELECT COUNT(*)
            FROM router_logs
            {where}
            """,
            *values
        )
        rows = await conn.fetch(
            f"""
            SELECT *
            FROM router_logs
            {where}
            {order_clause}
            LIMIT ${index}
            OFFSET ${index+1}
            """,
            *values,
            page_size,
            offset
        )

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size,
        "data": [dict(row) for row in rows]
    }