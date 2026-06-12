import pytest

def test_apply_indexes(cursor):
    """ TODO 2.1: Đọc và thực thi file 002_indexes.sql """
    with open("storage/migrations/002_indexes.sql", "r", encoding="utf-8") as f:
        sql_script = f.read()
    cursor.execute(sql_script)

def test_indexes_are_active(cursor):
    """ TODO 2.2: Kiểm tra các Index đã bám vào bảng đúng thiết kế chưa """
    cursor.execute("""
        SELECT indexname FROM pg_indexes 
        WHERE tablename IN ('sim_swap_events', 'device_swap_events');
    """)
    indexes = [row[0] for row in cursor.fetchall()]
    
    # Khẳng định các index cốt lõi phải tồn tại
    assert "idx_sim_swap_query" in indexes
    assert "idx_device_swap_query" in indexes