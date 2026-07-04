import os
import csv
import argparse
import random
from datetime import timedelta
from typing import Dict, List
from shared.seed_config import MASTER_SEED, SUBSCRIBER_POOL_SIZE, RADIUS_SIMULATION_START
from shared.subscriber_pool import (
    base_subscriber, has_sim_swap, swap_new_imsi_subscriber,
    base_device_imei, has_device_swap, device_swap_new_imei_subscriber,
)

SUBSCRIBERS_BY_IMSI: Dict[str, List[dict]] = {}
SUBSCRIBERS_BY_MSISDN: Dict[str, List[dict]] = {}
DEVICES_BY_MSISDN: Dict[str, List[dict]] = {}


def generate_mock_subscribers_csv(output_path: str, count: int = SUBSCRIBER_POOL_SIZE,
                                    seed_value: int = MASTER_SEED):
    rng = random.Random(seed_value)
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
                reg_date.strftime("%Y-%m-%dT%H:%M:%SZ"), 
                up_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            ])

            if has_sim_swap(i):
                swap_sub = swap_new_imsi_subscriber(i, count)
                # [FIX Bug B] swap PHẢI xảy ra sau đăng ký gốc — trước đây
                # swap_date = RADIUS_SIMULATION_START + random(1..180) độc lập
                # với reg_date = base_time + random(0..365) -> 74.5% trường hợp
                # swap_date < reg_date -> mock trả nhầm "initial_activation".
                swap_date = reg_date + timedelta(days=rng.randint(1, 60))
                writer.writerow([
                    swap_sub["imsi"], sub["msisdn"], "active", "452", "01", "Viettel",
                    "true", "false", "true",
                    reg_date.strftime("%Y-%m-%dT%H:%M:%SZ"), 
                    up_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
                ])

            if rng.random() < 0.02:
                port_sub = base_subscriber(count * 2 + i)
                port_date = up_date + timedelta(days=rng.randint(30, 180))
                writer.writerow([
                    sub["imsi"], port_sub["msisdn"], "active", "452", "01", "Viettel",
                    "true", "false", "true",
                    port_date.strftime("%Y-%m-%dT%H:%M:%SZ"), port_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
                ])


def generate_device_history_csv(output_path: str, count: int = SUBSCRIBER_POOL_SIZE,
                                  seed_value: int = MASTER_SEED):
    """[MỚI] Lịch sử gán IMEI theo msisdn — nguồn xác minh Conflict D.
    RNG dùng seed_value + 1, TÁCH RIÊNG khỏi generate_mock_subscribers_csv
    để không làm lệch sequence IMSI/MSISDN đã seed trước đó."""
    rng = random.Random(seed_value + 1)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["msisdn", "imei", "assigned_at"])

        for i in range(count):
            msisdn = base_subscriber(i)["msisdn"]
            assigned_date = RADIUS_SIMULATION_START + timedelta(
                days=rng.randint(0, 365), hours=rng.randint(0, 23)
            )
            writer.writerow([msisdn, base_device_imei(i), assigned_date.strftime("%Y-%m-%dT%H:%M:%SZ")])

            if has_device_swap(i):
                new_imei = device_swap_new_imei_subscriber(i, count)
                swap_date = assigned_date + timedelta(days=rng.randint(1, 60))
                writer.writerow([msisdn, new_imei, swap_date.strftime("%Y-%m-%dT%H:%M:%SZ")])


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
                "imsi": imsi, "msisdn": msisdn, "status": row["status"],
                "mcc": row["mcc"], "mnc": row["mnc"], "operator": row["operator"],
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


def load_devices_to_memory(file_path: str = "mock_services/hlr_hss/data/device_history.csv"):
    """[MỚI]"""
    global DEVICES_BY_MSISDN
    DEVICES_BY_MSISDN.clear()

    if not os.path.exists(file_path):
        print(f" Warning: Device history file not found at {file_path}")
        return

    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            DEVICES_BY_MSISDN.setdefault(row["msisdn"], []).append({
                "imei": row["imei"],
                "assigned_at": row["assigned_at"],
            })


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=SUBSCRIBER_POOL_SIZE)
    parser.add_argument("--seed", type=int, default=MASTER_SEED)
    args = parser.parse_args()
    generate_mock_subscribers_csv("mock_services/hlr_hss/data/subscribers.csv", args.count, args.seed)
    generate_device_history_csv("mock_services/hlr_hss/data/device_history.csv", args.count, args.seed)
    print(f" Generated {args.count} mock subscribers + device history (seed={args.seed})")