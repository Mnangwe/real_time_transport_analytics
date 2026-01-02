import pytest
import psycopg2
from contextlib import contextmanager
from dotenv import load_dotenv
import os

load_dotenv()

@pytest.fixture(scope="session")
def db_connection():
    """Provide database connection for tests"""
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        database=os.getenv("POSTGRES_DB", 5432),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    yield conn
    conn.close()


@contextmanager
def db_cursor(db_connection):
    """Provide a transactional scope for database operations"""
    cursor = db_connection.cursor()
    try:
        yield cursor
        db_connection.commit()
    except Exception:
        db_connection.rollback()
        raise
    finally:
        cursor.close()
