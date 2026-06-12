import pytest
import os
import csv
from simulator.simulator import RadiusSimulator

def test_simulator_end_to_end_execution(base_config):
    # Đường dẫn file kết xuất đầu ra của kịch bản test
    output_file = base_config.output
    
    # Kích hoạt thực thi mô phỏng tích hợp ngắt kết nối Kafka thực tế
    simulator = RadiusSimulator(base_config)
    simulator.execute_simulation()
    
    # Xác thực file CSV được sinh ra thành công
    assert os.path.exists(output_file)
    
    # Đọc cấu trúc file kiểm tra số lượng dòng và tiêu đề header
    with open(output_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        records = list(reader)
        
        assert len(records) == 50
        assert "acct_session_id" in records[0]
        assert "ingest_timestamp" in records[0]
        assert "framed_ip" in records[0]
        
    # Tiến hành dọn dẹp sạch tài nguyên tệp tin rác sau khi test hoàn thành
    if os.path.exists(output_file):
        os.remove(output_file)
        try:
            os.rmdir(os.path.dirname(output_file))
        except:
            pass