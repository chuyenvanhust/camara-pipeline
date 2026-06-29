#tests\unit\pipeline\conflict_resolution\test_resolver.py
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pipeline_v1.conflict_resolution.resolver import ConflictResolver

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("Testing-Conflict-Resolver") \
        .getOrCreate()

@pytest.fixture
def radius_schema():
    return StructType([
        StructField("acct_session_id", StringType(), True),
        StructField("imsi", StringType(), True),
        StructField("msisdn", StringType(), True),
        StructField("acct_status_type", StringType(), True),
        StructField("event_timestamp", TimestampType(), True)
    ])

def test_conflict_resolver_scenario_a_b_c(spark, radius_schema):
    """
    Unit Test: Inject thủ công tập dữ liệu chứa cả 3 loại Conflict A, B, C 
    để verify thuật toán phân loại và định tuyến đầu ra chính xác.
    """
    input_data = [
        # --- Kịch bản lỗi A (Session Inconsistency): Giữ dòng 1 (Start), dòng 2 (Stop) biến đổi IMSI bị đánh lỗi A ---
        ("SESS_A01", "IMSI_111", "9999111", "Start", datetime.fromisoformat("2026-06-14 10:00:00")),
        ("SESS_A01", "IMSI_999", "9999111", "Stop", datetime.fromisoformat("2026-06-14 10:05:00")), 

        # --- Kịch bản lỗi B (Double Active Session): Cùng 1 IMSI kích hoạt 2 dòng Start song song ---
        ("SESS_B01", "IMSI_222", "9999222", "Start", datetime.fromisoformat("2026-06-14 11:00:00")), # Giữ lại dòng này vì đến trước
        ("SESS_B02", "IMSI_222", "9999222", "Start", datetime.fromisoformat("2026-06-14 11:15:00")), # Đánh lỗi B vì trùng lặp Start

        # --- Kịch bản lỗi C (SIM Swap Signal): Cùng MSISDN đổi sang một IMSI hoàn toàn mới ---
        ("SESS_C01", "IMSI_333", "9999333", "Start", datetime.fromisoformat("2026-06-14 12:00:00")), # Bản ghi gốc ban đầu
        ("SESS_C02", "IMSI_444", "9999333", "Start", datetime.fromisoformat("2026-06-14 12:30:00"))  # Giữ lại cả 2 ở luồng sạch, nhưng ghi nhận tín hiệu C
    ]
    
    df = spark.createDataFrame(input_data, schema=radius_schema)
    
    # Thực thi hàm xử lý logic lõi
    clean_df, conflict_log_df = ConflictResolver.resolve_conflicts(df)
    
    clean_results = clean_df.collect()
    log_results = conflict_log_df.collect()
    
    # --- VERIFY LUỒNG SẠCH (CLEAN_DF) ---
    # Phải chứa 4 bản ghi: Dòng đầu của A, dòng đầu của B, và CẢ 2 DÒNG của kịch bản C
    assert len(clean_results) == 4
    clean_sessions = [row["acct_session_id"] for row in clean_results]
    assert "SESS_A01" in clean_sessions
    assert "SESS_B01" in clean_sessions
    assert "SESS_C01" in clean_sessions
    assert "SESS_C02" in clean_sessions
    assert "SESS_B02" not in clean_sessions # B02 bắt buộc phải bị loại khỏi luồng sạch

    # --- VERIFY LUỒNG NHẬT KÝ LỖI (CONFLICT_LOG_DF) ---
    assert len(log_results) == 3 # Gồm 1 dòng loại A, 1 dòng loại B, và 1 dòng tín hiệu C hoán đổi
    
    conflict_map = {row["acct_session_id"]: row["conflict_type"] for row in log_results}
    assert conflict_map["SESS_A01"] == "A"
    assert conflict_map["SESS_B02"] == "B"
    assert conflict_map["SESS_C02"] == "C"