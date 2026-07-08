import asyncpg

DATABASE_URL = "postgresql://postgres:password123@localhost:5432/postgres"

async def initdb():
    
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS router_logs (
            id BIGSERIAL PRIMARY KEY,
            source_device_ip INET,
            priority INTEGER,
            timestamp TEXT,
            router TEXT,
            action TEXT,
            incoming_interface TEXT,
            outgoing_interface TEXT,
            connection_state TEXT,
            protocol TEXT,
            source_ip INET,
            source_port INTEGER,
            destination_ip INET,
            destination_port INTEGER,
            packet_length INTEGER,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)

    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_router_logs_source_ip
        ON router_logs(source_ip);
    """)

    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_router_logs_destination_ip
        ON router_logs(destination_ip);
    """)

    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_router_logs_src_dst_ip
        ON router_logs(source_ip, destination_ip);
    """)

    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_router_logs_ports
        ON router_logs(source_port, destination_port);
    """)

    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_router_logs_protocol
        ON router_logs(protocol);
    """)

    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_router_logs_created_at
        ON router_logs(created_at);
    """)

    return conn

async def batch_insert_router_logs(conn, logs):

    if not logs:
        return

    records = [
        (
            log.get("source_device_ip"),
            log.get("priority"),
            log.get("timestamp"),
            log.get("router"),
            log.get("action"),
            log.get("incoming_interface"),
            log.get("outgoing_interface"),
            log.get("connection_state"),
            log.get("protocol"),
            log.get("source_ip"),
            log.get("source_port"),
            log.get("destination_ip"),
            log.get("destination_port"),
            log.get("packet_length"),
        )
        for log in logs
    ]

    await conn.copy_records_to_table(
        "router_logs",
        records=records,
        columns=[
            "source_device_ip",
            "priority",
            "timestamp",
            "router",
            "action",
            "incoming_interface",
            "outgoing_interface",
            "connection_state",
            "protocol",
            "source_ip",
            "source_port",
            "destination_ip",
            "destination_port",
            "packet_length",
        ]
    )


async def delete_old_router_logs(connection, retention_days: int = 180):
    """Delete router logs older than retention_days. Returns number of rows deleted."""
    
    result = await connection.execute(
        """
        DELETE FROM router_logs
        WHERE created_at < NOW() - ($1 || ' days')::interval
        """,
        str(retention_days),
    )
    # asyncpg returns a string like "DELETE 1234"
    try:
        return int(result.split()[-1])
    except (ValueError, IndexError):
        return None