#tests\unit\pipeline\deduplication\test_state_manager.py
import pytest
from pyspark.sql import SparkSession
from pipeline_v1.deduplication.state_manager import DedupStateManager
from pyspark.sql.types import LongType

def test_configure_rocksdb_applies_correct_configs(spark: SparkSession):
    """
    Unit Test: Đảm bảo State Manager nạp đúng cấu hình RocksDB vào Spark Session config.
    """
    test_checkpoint = "hdfs:///tmp/test_checkpoint"
    
    # Thực thi cấu hình
    DedupStateManager.configure_rocksdb(spark, test_checkpoint)
    
    # Kiểm tra xem config của Spark có chứa đúng Provider Class của RocksDB không
    provider_class = spark.conf.get("spark.sql.streaming.stateStore.providerClass")
    assert "RocksDBStateStoreProvider" in provider_class


def test_dedup_key_constants_are_correct():
    """
    Unit Test: Đảm bảo các trường khóa dùng để phân biệt trùng lặp không bị thay đổi sai lệch.
    """
    expected_keys = ["acct_session_id", "acct_status_type"]
    assert DedupStateManager.DEDUP_KEY_FIELDS == expected_keys
    assert DedupStateManager.TTL_SECONDS == 3600

def test_get_state_schema_returns_correct_schema():
    """
    Unit Test: Đảm bảo hàm get_state_schema trả về đúng schema cho dữ liệu trạng thái.
    """
    schema = DedupStateManager.get_state_schema()
    assert len(schema.fields) == 1
    assert schema.fields[0].name == "first_seen_timestamp"
    assert schema.fields[0].dataType == LongType()

  
