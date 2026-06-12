import pytest
import psycopg2
import os

@pytest.fixture(scope="session")
def db_connection():
    """ Fixture khởi tạo kết nối tới Postgres Docker Test """
    # Đọc cấu hình từ biến môi trường hoặc dùng mặc định của Container đơn lẻ
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "camara_network"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
        port=os.getenv("DB_PORT", "5432")
    )
    conn.autocommit = True
    yield conn
    conn.close()

@pytest.fixture(scope="function")
def cursor(db_connection):
    """ Fixture cung cấp cursor cho từng test case và rollback/clean nếu cần """
    cur = db_connection.cursor()
    yield cur
    cur.close()