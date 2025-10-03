import os
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from typing import Optional

DB_HOST  = os.environ["DB_HOST"]
DB_PORT  = int(os.environ.get("DB_PORT", 5432))
DB_USER  = os.environ["DB_USER"]
DB_PASS  = os.environ["DB_PASS"]
DB_NAME  = os.environ["DB_NAME"]
SCHEMA   = os.environ.get("SCHEMA", "tel_me")  # uses env if set

TABLES_IN_SCOPE = [
    "inforec_conditions",
    "inforec_general",
    "inforecord_price_qty_scales",
    "inforecord_purchasing_org",
]

_POOL: Optional[pool.SimpleConnectionPool] = None

def _get_pool() -> pool.SimpleConnectionPool:
    global _POOL
    if _POOL is None:
        _POOL = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=int(os.environ.get("MAX_CONNS", "8")),
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            dbname=DB_NAME,
            connect_timeout=5,
        )
    return _POOL

@contextmanager
def borrow_conn():
    pool_ = _get_pool()
    conn = pool_.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {SCHEMA};")
            cur.execute("SET statement_timeout TO '55s';")
        yield conn
    finally:
        pool_.putconn(conn)

def build_schema_block() -> str:
    ph = ", ".join("%s" for _ in TABLES_IN_SCOPE)
    sql = f"""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name IN ({ph})
        ORDER BY table_name, ordinal_position
    """
    lines, last, cols = [], None, []
    with borrow_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, [SCHEMA, *TABLES_IN_SCOPE])
        for tbl, col, dtype in cur.fetchall():
            if tbl != last and last:
                lines.append(f"### TABLE {last}\n" + ", ".join(cols))
                cols = []
            last = tbl
            cols.append(f"{col} ({dtype})")
        if last:
            lines.append(f"### TABLE {last}\n" + ", ".join(cols))
    return "\n\n".join(lines)
