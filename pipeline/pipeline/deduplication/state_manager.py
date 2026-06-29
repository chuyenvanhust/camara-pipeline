#!/usr/bin/env python3
# pipeline/deduplication/state_manager.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType

class DedupStateManager:
    """
    Quản lý cấu hình và trạng thái cho Stateful Deduplication sử dụng RocksDB.
    """
    
    # Định nghĩa các hằng số cấu hình hệ thống
    TTL_SECONDS = 3600  # 1 giờ
    DEDUP_KEY_FIELDS = ["acct_session_id", "acct_status_type"]

    @staticmethod
    def configure_rocksdb(spark: SparkSession, checkpoint_dir: str):
        """
        Cấu hình SparkSession để sử dụng RocksDB làm State Store Provider 
        và thiết lập thư mục lưu trữ checkpoint mặc định cho tầng stateful.
        """
        # Kích hoạt bộ lưu trữ RocksDB (giúp tối ưu RAM khi lưu lượng State lớn lên đến hàng triệu key)
        spark.conf.set(
            "spark.sql.streaming.stateStore.providerClass", 
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
        )
        # Thiết lập vị trí lưu vết checkpoint để phục hồi trạng thái khi job bị crash/restart
        spark.conf.set(
            "spark.sql.streaming.checkpointLocation", 
            f"{checkpoint_dir}/dedup/"
        )

    @staticmethod
    def get_state_schema() -> StructType:
        """
        Trả về StructType schema đại diện cho cấu trúc dữ liệu lưu trong RocksDB.
        Lưu trữ epoch millisecond (LongType) của bản ghi gốc đầu tiên để phục vụ tính toán TTL.
        """
        return StructType([
            StructField("first_seen_timestamp", LongType(), True)
        ])