import os
import csv
import random
import argparse
from typing import Dict
from .models import TacRecord
from shared.seed_config import MASTER_SEED, TAC_POOL_SIZE, RADIUS_SIMULATION_START 
from shared.tac_pool import generate_tac_codes

TAC_IN_MEMORY_DB: Dict[str, TacRecord] = {}


def generate_mock_csv(file_path: str, count: int = TAC_POOL_SIZE, seed_value: int = MASTER_SEED):
    """Sinh dữ liệu TAC giả lập ra CSV. TAC lấy từ generate_tac_codes()
    (shared) để đảm bảo khớp 100% với fallback pool của generator."""
    rng = random.Random(seed_value)  # RNG cục bộ, không đụng global
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    manufacturers = {
        "smartphone": ["Samsung", "Apple", "Xiaomi", "Oppo", "Vivo", "Realme"],
        "tablet": ["Apple", "Samsung", "Lenovo", "Huawei"],
        "router": ["Huawei", "TP-Link", "Netgear", "ZTE"],
        "iot": ["Quectel", "SimCom", "Sierra Wireless", "Telit"],
    }
    os_map = {
        "Samsung": "Android", "Apple": "iOS", "Xiaomi": "Android", "Oppo": "Android",
        "Vivo": "Android", "Realme": "Android", "Lenovo": "Android", "Huawei": "HarmonyOS",
        "TP-Link": "Embedded Linux", "Netgear": "Embedded Linux", "ZTE": "Embedded Linux",
        "Quectel": "RTOS", "SimCom": "RTOS", "Sierra Wireless": "Embedded Linux", "Telit": "RTOS",
    }

    tac_codes = generate_tac_codes(seed_value, count)  # nguồn TAC duy nhất

    with open(file_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["tac_code", "manufacturer", "model", "device_type",
                          "operating_system", "band_support", "approved_date", "status"])

        for tac in tac_codes:
            p = rng.random()
            if p < 0.60:
                dev_type = "smartphone"
            elif p < 0.80:
                dev_type = "tablet"
            elif p < 0.90:
                dev_type = "router"
            else:
                dev_type = "iot"

            mfr = rng.choice(manufacturers[dev_type])
            model = f"{mfr} X-{rng.randint(10, 99)}" if mfr != "Apple" else f"iPhone {rng.randint(11, 15)}"
            if dev_type == "tablet" and mfr == "Apple":
                model = f"iPad Mini {rng.randint(4, 6)}"

            bands = ["GSM", "WCDMA"]
            if dev_type in ["smartphone", "tablet", "router"]:
                bands.append("LTE")
            if rng.random() > 0.4:
                bands.append("NR")

            app_date = f"202{rng.randint(0, 5)}-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}"

            writer.writerow([tac, mfr, model, dev_type, os_map[mfr],
                              ";".join(bands), app_date, "active"])


def load_tac_csv_to_memory(file_path: str = "mock_services/gsma_tac/data/tac_records.csv"):
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
                status=row["status"].strip(),
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=TAC_POOL_SIZE)
    parser.add_argument("--seed", type=int, default=MASTER_SEED)
    args = parser.parse_args()
    generate_mock_csv("mock_services/gsma_tac/data/tac_records.csv", args.count, args.seed)
    print(f"✅ Generated {args.count} mock TAC records (seed={args.seed})")