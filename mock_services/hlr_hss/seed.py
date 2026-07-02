import os
import csv
import argparse
import random
from datetime import datetime, timedelta
from typing import Dict, List
from shared.seed_config import MASTER_SEED, SUBSCRIBER_POOL_SIZE, RADIUS_SIMULATION_START 
from shared.subscriber_pool import base_subscriber, has_sim_swap, swap_new_imsi_subscriber

SUBSCRIBERS_BY_IMSI: Dict[str, List[dict]] = {}
SUBSCRIBERS_BY_MSISDN: Dict[str, List[dict]] = {}


def generate_mock_subscribers_csv(output_path: str, count: int = SUBSCRIBER_POOL_SIZE,
                                    seed_value: int = MASTER_SEED):
    """Sinh subscribers.csv. IMSI/MSISDN gốc dùng base_subscriber() (shared)
    -> khớp 100% với simulator/generator.py.

    [FIX Conflict C] SIM Swap KHÔNG còn quyết định bằng rng.random() < 0.02
    (RNG riêng, simulator không thể biết trước) -- thay bằng has_sim_swap(i),
    công thức xác định dùng CHUNG với simulator, đảm bảo 2 bên luôn đồng bộ
    100% subscriber nào có swap và IMSI mới là gì, không cần đọc file của
    nhau lúc chạy.

    Number Portability vẫn giữ random (không liên quan Conflict C /
    SwapDetector, không cần đồng bộ với simulator)."""
    rng = random.Random(seed_value)  # RNG cục bộ, chỉ còn dùng cho registered_at/portability
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    headers = [
        "imsi", "msisdn", "status", "mcc", "mnc", "operator",
        "data_enabled", "roaming_enabled", "volte_enabled",
        "registered_at", "last_updated",
    ]
    base_time = RADIUS_SIMULATION_START 

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for i in range(count):
            sub = base_subscriber(i)
            reg_date = base_time + timedelta(days=rng.randint(0, 365), hours=rng.randint(0, 23))
            up_date = reg_date + timedelta(days=rng.randint(1, 100))

            writer.writerow([
                sub["imsi"], sub["msisdn"], "active", "452", "01", "Viettel",
                "true", "false", "true",
                reg_date.isoformat() + "Z", up_date.isoformat() + "Z",
            ])

          
            if has_sim_swap(i):
                swap_sub = swap_new_imsi_subscriber(i, count)
                
                swap_date = RADIUS_SIMULATION_START - timedelta(days=rng.randint(1, 180))
                writer.writerow([
                    swap_sub["imsi"], sub["msisdn"], "active", "452", "01", "Viettel",
                    "true", "false", "true",
                    swap_date.isoformat() + "Z", swap_date.isoformat() + "Z",
                ])

        
            if rng.random() < 0.02:
                port_sub = base_subscriber(count * 2 + i)
                port_date = up_date + timedelta(days=rng.randint(30, 180))
                writer.writerow([
                    sub["imsi"], port_sub["msisdn"], "active", "452", "01", "Viettel",
                    "true", "false", "true",
                    port_date.isoformat() + "Z", port_date.isoformat() + "Z",
                ])


def load_subscribers_to_memory(file_path: str = "mock_services/hlr_hss/data/subscribers.csv"):
    global SUBSCRIBERS_BY_IMSI, SUBSCRIBERS_BY_MSISDN
    SUBSCRIBERS_BY_IMSI.clear()
    SUBSCRIBERS_BY_MSISDN.clear()

    if not os.path.exists(file_path):
        print(f" Warning: Subscriber DB file not found at {file_path}")
        return

    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            imsi, msisdn = row["imsi"], row["msisdn"]
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
                "last_updated": row["last_updated"],
            }
            SUBSCRIBERS_BY_IMSI.setdefault(imsi, []).append(profile)
            SUBSCRIBERS_BY_MSISDN.setdefault(msisdn, []).append(profile)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=SUBSCRIBER_POOL_SIZE)
    parser.add_argument("--seed", type=int, default=MASTER_SEED)
    args = parser.parse_args()
    generate_mock_subscribers_csv("mock_services/hlr_hss/data/subscribers.csv", args.count, args.seed)
    print(f" Generated {args.count} mock subscribers (seed={args.seed})")