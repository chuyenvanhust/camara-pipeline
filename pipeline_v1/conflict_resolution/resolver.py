#!/usr/bin/env python3
#pipeline\conflict_resolution\resolver.py
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from typing import Tuple

class ConflictResolver:
    """
    Spark Job nhận luồng dữ liệu từ `radius.dedup`, thực hiện phân loại 
    3 loại xung đột (A, B, C) theo thứ tự ưu tiên A -> B -> C.
    Hàm xử lý thuần túy trên DataFrame Engine (Không gọi Network/I/O).
    """
    
    @staticmethod
    def resolve_conflicts(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Xử lý phân loại xung đột từ luồng đầu vào.
        
        Args:
            df (DataFrame): Dữ liệu từ topic `radius.dedup` chứa các trường:
                            acct_session_id, imsi, msisdn, acct_status_type, event_timestamp, ...
                            
        Returns:
            tuple[DataFrame, DataFrame]: 
                - clean_df: DataFrame chứa các bản ghi sạch/hợp lệ (đã lọc bỏ A, B nhưng GIỮ LẠI C).
                - conflict_log_df: DataFrame chứa nhật ký các bản ghi lỗi phục vụ ghi conflict_log.
        """
        # Đảm bảo dữ liệu đã được cấu hình biên Watermark để tính toán Stateful Window nếu cần
        # (Giả định df đã được gắn watermark phía trước pipeline)
        
        # -------------------------------------------------------------------------
        # TODO 1: PHÂN LOẠI CONFLICT LOẠI A - Session Inconsistency
        # Điều kiện: Cùng acct_session_id nhưng imsi hoặc msisdn thay đổi giữa Start và Stop/Interim.
        # Chiến lược: Giữ bản ghi 'Start', đánh dấu các bản ghi còn lại của session đó là 'A'.
        # -------------------------------------------------------------------------
        # Gợi ý: Dùng Window partitionBy("acct_session_id").orderBy("event_timestamp") 
        # Hoặc dùng MapGroupsWithState nếu xử lý Stream thuần túy. Ở đây cấu hình giả định qua Window:
        
        window_session = Window.partitionBy("acct_session_id").orderBy("event_timestamp")
        
        # Lấy thông tin bản ghi đầu tiên (thường là Start) của mỗi Session để đối chiếu
        df_with_first = df \
            .withColumn("first_imsi", F.first("imsi").over(window_session)) \
            .withColumn("first_msisdn", F.first("msisdn").over(window_session)) \
            .withColumn("first_status", F.first("acct_status_type").over(window_session))

        df_conflict_a = df_with_first.withColumn(
            "is_conflict_a",
            F.when(
                (F.col("acct_status_type") != "Start") & 
                ((F.col("imsi") != F.col("first_imsi")) | (F.col("msisdn") != F.col("first_msisdn"))),
                True
            ).otherwise(False)
        )

        # -------------------------------------------------------------------------
        # TODO 2: PHÂN LOẠI CONFLICT LOẠI B - Double Active Session
        # Điều kiện: Cùng imsi có 2 gói 'Start' chưa có 'Stop' tương ứng tại cùng thời điểm.
        # Chiến lược: Ưu tiên giữ session có event_timestamp nhỏ hơn, đánh dấu session sau là 'B'.
        # LƯU Ý: Bản ghi đã bị đánh dấu 'A' thì KHÔNG xét 'B' nữa (Ưu tiên A -> B -> C).
        # -------------------------------------------------------------------------
        window_imsi_start = Window.partitionBy("imsi").orderBy("event_timestamp")
        
        df_conflict_b = df_conflict_a.withColumn(
            "is_conflict_b",
            F.when(
                (F.col("is_conflict_a") == False) & 
                (F.col("acct_status_type") == "Start") & 
                (F.row_number().over(window_imsi_start) > 1), # Gói Start thứ 2 trở đi của cùng 1 IMSI
                True
            ).otherwise(False)
        )

        # -------------------------------------------------------------------------
        # TODO 3: PHÂN LOẠI CONFLICT LOẠI C - MSISDN<->IMSI Remap (SIM Swap Signal)
        # Điều kiện: Cùng msisdn mapping sang imsi mới khác hoàn toàn imsi cũ trước đó.
        # Chiến lược: Giữ cả 2 bản ghi trong luồng sạch (vì đây là business hợp lệ), 
        # nhưng đồng thời trích xuất bản ghi này sang luồng nghi vấn để SwapDetector xử lý hậu kỳ.
        # -------------------------------------------------------------------------
        window_msisdn = Window.partitionBy("msisdn").orderBy("event_timestamp")
        
        df_all_tracked = df_conflict_b \
            .withColumn("prev_imsi", F.lag("imsi", 1).over(window_msisdn))
            
        df_final = df_all_tracked.withColumn(
            "is_conflict_c",
            F.when(
                (F.col("is_conflict_a") == False) & 
                (F.col("is_conflict_b") == False) & 
                (F.col("prev_imsi").isNotNull()) & 
                (F.col("imsi") != F.col("prev_imsi")),
                True
            ).otherwise(False)
        )

        # -------------------------------------------------------------------------
        # TODO 4: PHÂN LUỒNG OUTPUT DATA
        # -------------------------------------------------------------------------
        # Luồng log lỗi tập hợp các bản ghi dính lỗi hệ thống (A hoặc B) hoặc tín hiệu C để lưu vết
        conflict_log_df = df_final.filter(
            (F.col("is_conflict_a") == True) | 
            (F.col("is_conflict_b") == True) | 
            (F.col("is_conflict_c") == True)
        ).withColumn(
            "conflict_type",
            F.when(F.col("is_conflict_a") == True, "A")
             .when(F.col("is_conflict_b") == True, "B")
             .otherwise("C")
        ).select("acct_session_id", "msisdn", "imsi", "event_timestamp", "conflict_type")

        # Luồng dữ liệu sạch (Giữ lại bản ghi hợp lệ và các bản ghi thuộc diện hoán đổi SIM loại C)
        clean_df = df_final.filter((F.col("is_conflict_a") == False) & (F.col("is_conflict_b") == False))

        return clean_df, conflict_log_df