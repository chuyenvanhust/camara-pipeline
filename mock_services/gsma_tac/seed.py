import os
import csv
import random
import argparse
from typing import Dict
from mock_services.gsma_tac.models import TacRecord

# In-memory DB toàn cục
TAC_IN_MEMORY_DB: Dict[str, TacRecord] = {}

def generate_mock_csv(file_path: str, count: int, seed_value: int):
    """Sinh dữ liệu giả lập 2000 bản ghi xuất ra file CSV"""
    random.seed(seed_value)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    manufacturers = {
        "smartphone": ["Samsung", "Apple", "Xiaomi", "Oppo", "Vivo", "Realme"],
        "tablet": ["Apple", "Samsung", "Lenovo", "Huawei"],
        "router": ["Huawei", "TP-Link", "Netgear", "ZTE"],
        "iot": ["Quectel", "SimCom", "Sierra Wireless", "Telit"]
    }
    
    os_map = {"Samsung": "Android", "Apple": "iOS", "Xiaomi": "Android", "Oppo": "Android", 
              "Vivo": "Android", "Realme": "Android", "Lenovo": "Android", "Huawei": "HarmonyOS",
              "TP-Link": "Embedded Linux", "Netgear": "Embedded Linux", "ZTE": "Embedded Linux",
              "Quectel": "RTOS", "SimCom": "RTOS", "Sierra Wireless": "Embedded Linux", "Telit": "RTOS"}

    used_tacs = set()
    
    with open(file_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["tac_code", "manufacturer", "model", "device_type", "operating_system", "band_support", "approved_date", "status"])
        
        for _ in range(count):
            while True:
                tac = f"{random.randint(100000, 999999)}"
                if tac not in used_tacs:
                    used_tacs.add(tac)
                    break
            
            p = random.random()
            if p < 0.60: dev_type = "smartphone"
            elif p < 0.80: dev_type = "tablet"
            elif p < 0.90: dev_type = "router"
            else: dev_type = "iot"
            
            mfr = random.choice(manufacturers[dev_type])
            model = f"{mfr} X-{random.randint(10, 99)}" if mfr != "Apple" else f"iPhone {random.randint(11, 15)}"
            if dev_type == "tablet" and mfr == "Apple": model = f"iPad Mini {random.randint(4, 6)}"
            
            bands = ["GSM", "WCDMA"]
            if dev_type in ["smartphone", "tablet", "router"]: bands.append("LTE")
            if random.random() > 0.4: bands.append("NR")
            
            app_date = f"202{random.randint(0,5)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
            
            writer.writerow([
                tac, mfr, model, dev_type, os_map[mfr],
                ";".join(bands), app_date, "active"
            ])

def load_tac_csv_to_memory(file_path: str = "mock_services/gsma_tac/data/tac_records.csv"):
    """Nạp dữ liệu từ file CSV tĩnh vào In-Memory Dictionary"""
    global TAC_IN_MEMORY_DB
    TAC_IN_MEMORY_DB.clear()
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Database file not found at {file_path}")
        
    with open(file_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tac_code = row["tac_code"].strip()
            TAC_IN_MEMORY_DB[tac_code] = TacRecord(
                tac=tac_code,
                manufacturer=row["manufacturer"].strip(),
                model=row["model"].strip(),
                device_type=row["device_type"].strip(),
                operating_system=row["operating_system"].strip(),
                band_support=row["band_support"].strip().split(";"),
                approved_date=row["approved_date"].strip(),
                status=row["status"].strip()
            )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    generate_mock_csv("mock_services/gsma_tac/data/tac_records.csv", args.count, args.seed)
    print(f"✅ Generated {args.count} mock records to data/tac_records.csv")