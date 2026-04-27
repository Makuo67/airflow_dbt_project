# src/utils/db.py
import MySQLdb
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from src.utils.config import MYSQL_CONFIG, POSTGRES_CONFIG


@contextmanager
def mysql_connection():
    conn = MySQLdb.connect(
        host=MYSQL_CONFIG["host"],
        user=MYSQL_CONFIG["user"],
        passwd=MYSQL_CONFIG["password"],
        db=MYSQL_CONFIG["database"],
        local_infile=1,
        charset="utf8mb4",
        connect_timeout=30,
        autocommit=False
    )
    cursor = conn.cursor()
    try:
        yield conn, cursor
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.commit()
        cursor.close()
        conn.close()


@contextmanager
def postgres_connection():
    conn = psycopg2.connect(
        host=POSTGRES_CONFIG["host"],
        database=POSTGRES_CONFIG["database"],
        user=POSTGRES_CONFIG["user"],
        password=POSTGRES_CONFIG["password"],
        port=POSTGRES_CONFIG["port"],
        connect_timeout=30
    )
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield conn, cursor
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.commit()
        cursor.close()
        conn.close()
