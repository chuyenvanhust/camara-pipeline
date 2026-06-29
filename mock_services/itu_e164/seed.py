#mock_services\itu_e164\seed.py
import os
import csv
from typing import Dict, Set

# In-memory Storage lưu cấu trúc định tuyến tĩnh
# COUNTRY_DB: { "84": "Vietnam", "1": "USA/Canada" }
COUNTRY_DB: Dict[str, str] = {}
# OPERATOR_DB: { "84": { "91", "90", "98" }, "1": { "202", "703" } }
OPERATOR_DB: Dict[str, Set[str]] = {}

def generate_mock_itu_csv():
    """Tự động sinh 2 file CSV tĩnh nếu chưa tồn tại trong thư mục data/"""
    data_dir = "mock_services/itu_e164/data"
    os.makedirs(data_dir, exist_ok=True)
    
    cc_path = os.path.join(data_dir, "country_codes.csv")
    op_path = os.path.join(data_dir, "operator_prefixes.csv")
    
    if not os.path.exists(cc_path):
        with open(cc_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["country_code", "country_name"])
            writer.writerows([["84", "Vietnam"], ["1", "USA"], ["44", "UK"]])
            
    if not os.path.exists(op_path):
        with open(op_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["country_code", "prefix"])
            writer.writerows([["84", "91"], ["84", "90"], ["84", "98"], ["1", "202"], ["44", "77"]])

def load_itu_data_to_memory(cc_path: str = "mock_services/itu_e164/data/country_codes.csv", 
                            op_path: str = "mock_services/itu_e164/data/operator_prefixes.csv"):
    """Nạp dữ liệu từ 2 file CSV vào RAM (Hỗ trợ truyền path linh hoạt khi test đối xứng)"""
    global COUNTRY_DB, OPERATOR_DB
    COUNTRY_DB.clear()
    OPERATOR_DB.clear()
    
    # Kiểm tra sự tồn tại của cả 2 file trước khi đọc
    if not os.path.exists(cc_path) or not os.path.exists(op_path):
        print(f"⚠️ Warning: ITU CSV files not found at {cc_path} or {op_path}")
        return
    
    # Nạp dữ liệu Quốc gia (Country Codes)
    with open(cc_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            COUNTRY_DB[row["country_code"].strip()] = row["country_name"].strip()
            
    # Nạp dữ liệu Nhà mạng (Operator Prefixes)
    with open(op_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cc = row["country_code"].strip()
            pref = row["prefix"].strip()
            if cc not in OPERATOR_DB:
                OPERATOR_DB[cc] = set()
            OPERATOR_DB[cc].add(pref)