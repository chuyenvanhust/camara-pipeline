import os
import csv
import argparse
import random
from datetime import datetime, timedelta
from typing import Dict, List
import sqlite3

# In-memory RAM database để định tuyến siêu tốc O(1)
# SUBSCRIBERS_BY_IMSI: mỗi IMSI có thể có nhiều dòng lịch sử (number portability:
# 1 IMSI gắn nhiều MSISDN theo thời gian) -> giữ List để build msisdn-history.
SUBSCRIBERS_BY_IMSI: Dict[str, List[dict]] = {}
SUBSCRIBERS_BY_MSISDN: Dict[str, List[dict]] = {}




def generate_mock_subscribers_csv(output_path: str, count: int, seed_value: int):
    """Sinh file dữ liệu subscribers.csv đồng bộ giả lập dựa trên seed cố định"""
    random.seed(seed_value)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    headers = [
        "imsi", "msisdn", "status", "mcc", "mnc", "operator",
        "data_enabled", "roaming_enabled", "volte_enabled",
        "registered_at", "last_updated"
    ]
    
    base_time = datetime(2022, 1, 1, 8, 0, 0)
    
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        
        for i in range(count):
            imsi = f"452010{i:09d}"
            msisdn = f"+8497{i:07d}"
            reg_date = base_time + timedelta(days=random.randint(0, 365), hours=random.randint(0, 23))
            up_date = reg_date + timedelta(days=random.randint(1, 100))
            
            # Khởi tạo bản ghi gốc
            writer.writerow([
                imsi, msisdn, "active", "452", "01", "Viettel",
                "true", "false", "true",
                reg_date.isoformat() + "Z", up_date.isoformat() + "Z"
            ])
            
            # Tạo hiệu ứng SIM Swap ngẫu nhiên (khoảng 2% dữ liệu)
            # -> cùng MSISDN, IMSI mới. Phục vụ /subscribers/{msisdn}/imsi-history
            if random.random() < 0.02:
                swap_imsi = f"4520109{i:08d}"  # IMSI mới
                swap_date = up_date + timedelta(days=random.randint(30, 180))
                # Bản ghi swap gán MSISDN cũ sang IMSI mới
                writer.writerow([
                    swap_imsi, msisdn, "active", "452", "01", "Viettel",
                    "true", "false", "true",
                    swap_date.isoformat() + "Z", swap_date.isoformat() + "Z"
                ])

            # Tạo hiệu ứng Number Portability ngẫu nhiên (khoảng 2% dữ liệu, độc lập SIM Swap)
            # -> cùng IMSI, MSISDN mới. Phục vụ /subscribers/{imsi}/msisdn-history
            if random.random() < 0.02:
                ported_msisdn = f"+8498{i:07d}"  # MSISDN mới, prefix 98 tránh đụng prefix 97 gốc
                port_date = up_date + timedelta(days=random.randint(30, 180))
                # Bản ghi porting gán IMSI cũ sang MSISDN mới
                writer.writerow([
                    imsi, ported_msisdn, "active", "452", "01", "Viettel",
                    "true", "false", "true",
                    port_date.isoformat() + "Z", port_date.isoformat() + "Z"
                ])

def load_subscribers_to_memory(file_path: str = "mock_services/hlr_hss/data/subscribers.csv"):
    """Nạp dữ liệu từ file CSV vào RAM đa cấu trúc"""
    global SUBSCRIBERS_BY_IMSI, SUBSCRIBERS_BY_MSISDN
    SUBSCRIBERS_BY_IMSI.clear()
    SUBSCRIBERS_BY_MSISDN.clear()
    
    if not os.path.exists(file_path):
        print(f"⚠️ Warning: Subscriber DB file not found at {file_path}")
        return

    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            imsi = row["imsi"]
            msisdn = row["msisdn"]
            
            profile = {
                "imsi": imsi,
                "msisdn": msisdn,
                "status": row["status"],
                "mcc": row["mcc"],
                "mnc": row["mnc"],
                "operator": row["operator"],
                "service_profile": {
                    "data_enabled": row["data_enabled"].lower() == "true",
                    "roaming_enabled": row["roaming_enabled"].lower() == "true",
                    "volte_enabled": row["volte_enabled"].lower() == "true",
                },
                "registered_at": row["registered_at"],
                "last_updated": row["last_updated"]
            }
            
            # Nạp bảng tra cứu IMSI (Có thể chứa danh sách lịch sử number portability)
            if imsi not in SUBSCRIBERS_BY_IMSI:
                SUBSCRIBERS_BY_IMSI[imsi] = []
            SUBSCRIBERS_BY_IMSI[imsi].append(profile)
            
            # Nạp bảng tra cứu MSISDN (Có thể chứa danh sách lịch sử swap)
            if msisdn not in SUBSCRIBERS_BY_MSISDN:
                SUBSCRIBERS_BY_MSISDN[msisdn] = []
            SUBSCRIBERS_BY_MSISDN[msisdn].append(profile)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=100000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    generate_mock_subscribers_csv("mock_services/hlr_hss/data/subscribers.csv", args.count, args.seed)
    print(f"gen mock xong")