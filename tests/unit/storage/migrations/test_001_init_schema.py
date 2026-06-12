import pytest

def test_apply_init_schema(cursor):
    """ TODO 1.1: Đọc và thực thi file 001_init_schema.sql """
    with open("storage/migrations/001_init_schema.sql", "r", encoding="utf-8") as f:
        sql_script = f.read()
    
    # Thực thi không ném lỗi cú pháp
    cursor.execute(sql_script)

def test_tables_exist_and_partitioned(cursor):
    """ TODO 1.2: Kiểm tra xem các bảng gốc đã tồn tại trong DB chưa """
    # Query kiểm tra bảng hệ thống của Postgres
    cursor.execute("""
        SELECT relname, relkind 
        FROM pg_class 
        WHERE relname IN ('sim_swap_events', 'device_swap_events');
    """)
    results = cursor.fetchall()
    assert len(results) == 2
    for row in results:
        # relkind = 'p' nghĩa là bảng có thiết lập Partitioned (Phân vùng)
        assert row[1] == 'p', f"Bảng {row[0]} chưa được cấu hình Partition"