import pytest

def test_apply_partitions(cursor):
    """ TODO 3.1: Đọc và thực thi file 003_partitions.sql """
    with open("storage/migrations/003_partitions.sql", "r", encoding="utf-8") as f:
        sql_script = f.read()
    cursor.execute(sql_script)

def test_explain_analyze_index_scan(cursor):
    """ 
    TODO 3.2: Chạy EXPLAIN ANALYZE kịch bản API (Pattern 1) 
    Mục tiêu: Chứng minh Postgres thực sự dùng Index Scan thay vì Seq Scan.
    """
    query = """
        EXPLAIN (FORMAT JSON)
        SELECT * FROM sim_swap_events 
        WHERE phone_number = '+84912345678' 
        ORDER BY event_timestamp DESC LIMIT 1;
    """
    cursor.execute(query)
    explain_result = cursor.fetchone()[0] # Nhận kết quả dạng JSON định dạng cây
    
    # Chuyển đổi chuỗi kết quả để phân tích Plan
    plan_str = str(explain_result)
    
    # Khẳng định (Assert) chiến lược quét dữ liệu: Phải chứa chữ 'Index Scan'
    assert "Index Scan" in plan_str or "Bitmap Index Scan" in plan_str
    # Khẳng định không bị quét sạch cả bảng (Seq Scan là cấm kỵ với data lớn)
    assert "Sequential Scan" not in plan_str