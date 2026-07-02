#mock_services\itu_e164\seed.py
import os
import csv
from typing import Dict, Set
import argparse


COUNTRY_DB: Dict[str, str] = {}

OPERATOR_DB: Dict[str, Set[str]] = {}

def generate_mock_itu_csv():
    data_dir = "mock_services/itu_e164/data"
    os.makedirs(data_dir, exist_ok=True)
    
    cc_path = os.path.join(data_dir, "country_codes.csv")
    op_path = os.path.join(data_dir, "operator_prefixes.csv")
    
    # 1. Ghi dữ liệu Quốc gia
    with open(cc_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["country_code", "country_name"])
        writer.writerows([
            ["84", "Vietnam"], ["1", "USA"], ["44", "UK"], 
            ["61", "Australia"], ["81", "Japan"], ["82", "Korea"]
        ])
        
    # 2. Ghi dữ liệu Prefix đa dạng (Phủ từ 30 đến 99 để test mọi trường hợp)
    with open(op_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["country_code", "prefix"])
        
        # Thêm các đầu số thực tế của Việt Nam
        # Viettel: 3x, 86, 96, 97, 98 | Vina: 81, 82, 83, 84, 85, 88, 91, 94 | Mobi: 89, 90, 93
        prefixes_vn = [str(i) for i in range(30, 99)] # Phủ toàn bộ dải 30-99
        
        rows = [["84", p] for p in prefixes_vn]
        # Thêm một vài đầu số quốc tế khác
        rows.extend([["1", "202"], ["1", "415"], ["44", "77"], ["44", "78"]])
        
        writer.writerows(rows)
    
    print(" ITU CSV files generated with full prefix coverage (30-99).")

def load_itu_data_to_memory(cc_path: str = "mock_services/itu_e164/data/country_codes.csv", 
                            op_path: str = "mock_services/itu_e164/data/operator_prefixes.csv"):
    """Nạp dữ liệu từ 2 file CSV vào RAM (Hỗ trợ truyền path linh hoạt khi test đối xứng)"""
    global COUNTRY_DB, OPERATOR_DB
    COUNTRY_DB.clear()
    OPERATOR_DB.clear()
    
    # Kiểm tra sự tồn tại của cả 2 file trước khi đọc
    if not os.path.exists(cc_path) or not os.path.exists(op_path):
        print(f" Warning: ITU CSV files not found at {cc_path} or {op_path}")
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

if __name__ == "__main__":
    
    generate_mock_itu_csv()
    load_itu_data_to_memory()
    print(f"gen mock xong")