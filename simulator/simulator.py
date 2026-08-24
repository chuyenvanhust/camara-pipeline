import os
import csv
import sys
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.subscriber_pool import (
    base_device_imei, swap_new_imsi_subscriber, device_swap_new_imei_subscriber
)

from simulator.config import SimulatorConfig
from simulator.generators import RadiusDataGenerator
from simulator.error_injectors import ErrorInjector

class RadiusSimulator:
    def __init__(self, config: SimulatorConfig):
        self.config = config
        self.generator = RadiusDataGenerator(seed=config.seed)
        self.injector = ErrorInjector(config)

    def execute_simulation(self):
        print(" Starting RADIUS Accounting Log Simulator...")

        start_time = datetime(2026, 1, 1, 0, 0, 0)
        num_sessions = max(1, self.config.records // 2)
        records: List[Dict[str, Any]] = []

        # Persistent state dictionaries for active IMSI/IMEI per subscriber index
        active_imsi: Dict[int, str] = {}
        active_imei: Dict[int, str] = {}
        sessions_count: Dict[int, int] = {}

        sim_swaps_count = 0
        device_swaps_count = 0

        for s in range(num_sessions):
            sub_idx = s % self.config.subscribers
            
            # Initialize active state if it's the first session of the subscriber
            if sub_idx not in active_imsi:
                sub_data = self.generator.generate_base_subscriber(sub_idx)
                active_imsi[sub_idx] = sub_data["imsi"]
                active_imei[sub_idx] = base_device_imei(sub_idx)

            sub_data = self.generator.generate_base_subscriber(sub_idx)
            msisdn = sub_data["msisdn"]

            sub_sessions = sessions_count.get(sub_idx, 0)

            # Persistently inject swaps only if they have already connected at least once
            # This ensures that there is a pre-existing state in the DB to compare against.
            if sub_sessions > 0:
                r_sim = self.generator.rng.random()
                if r_sim < self.config.sim_swap_rate:
                    new_imsi = swap_new_imsi_subscriber(sub_idx, self.config.subscribers)["imsi"]
                    # new_imsi là giá trị CỐ ĐỊNH theo sub_idx (không đổi giữa các lần
                    # swap của cùng subscriber) -- nếu subscriber đã swap trước đó,
                    # lần roll thành công thứ 2 trở đi sẽ ra đúng giá trị đang active,
                    # tức KHÔNG có thay đổi thực trong dữ liệu. Chỉ đếm khi giá trị
                    # thực sự khác, để ground-truth khớp với số lần detection có thể
                    # phát hiện được từ so sánh imsi_old != imsi_new.
                    if new_imsi != active_imsi[sub_idx]:
                        sim_swaps_count += 1
                    active_imsi[sub_idx] = new_imsi

                r_dev = self.generator.rng.random()
                if r_dev < self.config.device_swap_rate:
                    new_imei = device_swap_new_imei_subscriber(sub_idx, self.config.subscribers)
                    if new_imei != active_imei[sub_idx]:
                        device_swaps_count += 1
                    active_imei[sub_idx] = new_imei

            sessions_count[sub_idx] = sub_sessions + 1

            imsi_current = active_imsi[sub_idx]
            imei_current = active_imei[sub_idx]

            session_id = f"SESS_{s:010d}"
            ggsn_name = f"GGSN_NODE_{(sub_idx % 5) + 1:02d}"

            t_start = start_time + timedelta(
                seconds=s * (86400 * self.config.days // num_sessions)
            )
            duration_seconds = 60 + (s % 3600)
            t_stop = t_start + timedelta(seconds=duration_seconds)
            framed_ip = f"10.100.{(sub_idx // 256) % 256}.{sub_idx % 256}"

            common = {
                "msisdn": msisdn,
                "Calling_Station_Id": msisdn,
                "imsi": imsi_current,
                "imei": imei_current,
                "rat_type": "E-UTRAN",
                "framed_ip": framed_ip,
                "Framed_IP_Address": framed_ip,
                "nas_ip": "192.168.1.1",
                "NAS_Identifier": ggsn_name,
                "nas_identifier": ggsn_name,
                "mcc_mnc": "452-01",
                "_sub_idx": sub_idx,
            }

            records.append({
                **common,
                "acct_session_id": session_id,
                "acct_status_type": "Start",
                "acct_session_time": "0",
                "event_timestamp": t_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "timestamp": t_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ingest_timestamp": (t_start + timedelta(seconds=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            })
            records.append({
                **common,
                "acct_session_id": session_id,
                "acct_status_type": "Stop",
                "acct_session_time": str(duration_seconds),
                "event_timestamp": t_stop.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "timestamp": t_stop.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ingest_timestamp": (t_stop + timedelta(seconds=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            })

        print(f" Persistent Injected Swaps Count: SIM Swaps = {sim_swaps_count}, Device Swaps = {device_swaps_count}")

        # 2. Tiêm lỗi/nghiệp vụ
        records = self.injector.inject_conflicts(records)
        records = self.injector.inject_invalid_imei(records)
        records = self.injector.inject_late_arrivals(records)
        records = self.injector.inject_duplicates(records)
        records = self.injector.inject_missing_fields(records)

        if self.config.kafka:
            self._stream_to_kafka(records)
        else:
            self._write_to_csv_file(records)

    def _write_to_csv_file(self, records: List[Dict[str, Any]]):
        """Ghi dữ liệu kết xuất ra tệp tin vật lý CSV"""
        os.makedirs(os.path.dirname(self.config.output), exist_ok=True)
        headers = [
            "acct_status_type", "acct_session_id", "acct_session_time",
            "event_timestamp", "ingest_timestamp", "timestamp", "msisdn", "Calling_Station_Id",
            "imsi", "imei", "rat_type", "framed_ip", "Framed_IP_Address", "nas_ip",
            "NAS_Identifier", "nas_identifier", "mcc_mnc"
        ]
        
        with open(self.config.output, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers, extrasaction='ignore')
            writer.writeheader()
            for rec in records:
                writer.writerow(rec)
                
        print(f" Simulation finished completely! {len(records)} records saved into file: {self.config.output}")

    def _stream_to_kafka(self, records: List[Dict[str, Any]]):
        """Stream trực tiếp vào Kafka Broker topic radius.accounting.raw"""
        import json
        try:
            from kafka import KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "camara-kafka:9092"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f" Streaming {len(records)} records directly into Kafka topic '{self.config.kafka_topic}'...")
            for rec in records:
                producer.send(self.config.kafka_topic, value=rec)
            producer.flush()
            print(" Stream completed successfully.")
        except Exception as e:
            print(f" Error streaming to Kafka: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RADIUS Accounting Stream Log Simulator CLI Engine")
    parser.add_argument("--records", type=int, default=2_000_000)
    parser.add_argument("--subscribers", type=int, default=100_000)
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--duplicate-rate", type=float, default=0.0)
    parser.add_argument("--late-arrival-rate", type=float, default=0.0)
    parser.add_argument("--invalid-imei-rate", type=float, default=0.0)
    parser.add_argument("--conflict-rate", type=float, default=0.0)
    parser.add_argument("--missing-field-rate", type=float, default=0.0)
    parser.add_argument("--sim-swap-rate", type=float, default=0.002)
    parser.add_argument("--device-swap-rate", type=float, default=0.002)
    parser.add_argument("--output", type=str, default="data/radius_log.csv")
    parser.add_argument("--kafka", action="store_true")
    parser.add_argument("--kafka-topic", type=str, default="radius.accounting.raw")
    
    args = parser.parse_args()
    
    config = SimulatorConfig(
        records=args.records, subscribers=args.subscribers, days=args.days, seed=args.seed,
        duplicate_rate=args.duplicate_rate, late_arrival_rate=args.late_arrival_rate,
        invalid_imei_rate=args.invalid_imei_rate, conflict_rate=args.conflict_rate,
        missing_field_rate=args.missing_field_rate, sim_swap_rate=args.sim_swap_rate,
        device_swap_rate=args.device_swap_rate, output=args.output,
        kafka=args.kafka, kafka_topic=args.kafka_topic
    )
    
    simulator = RadiusSimulator(config)
    simulator.execute_simulation()