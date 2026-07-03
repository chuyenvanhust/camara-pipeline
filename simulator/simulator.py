#simulator\simulator.py
import os
import csv
import sys
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
        self.generator.fetch_tac_pool_from_mock()

        start_time = datetime(2026, 1, 1, 0, 0, 0)

   
        num_sessions = max(1, self.config.records // 2)
        records: List[Dict[str, Any]] = []

        for s in range(num_sessions):
            sub_idx = s % self.config.subscribers
            sub = self.generator.generate_base_subscriber(sub_idx)
            imei = self.generator.generate_valid_imei()
            session_id = f"SESS_{s:010d}"

            t_start = start_time + timedelta(
                seconds=s * (86400 * self.config.days // num_sessions)
            )
            duration_seconds = 60 + (s % 3600) 
            t_stop = t_start + timedelta(seconds=duration_seconds)

            common = {
                "msisdn": sub["msisdn"],
                "imsi": sub["imsi"],
                "imei": imei,
                "rat_type": "E-UTRAN",
                "framed_ip": f"10.100.{(sub_idx // 256) % 256}.{sub_idx % 256}",
                "nas_ip": "192.168.1.1",
                "mcc_mnc": "452-01",
                "_sub_idx": sub_idx,
            }

            records.append({
                **common,
                "acct_session_id": session_id,
                "acct_status_type": "Start",
                "acct_session_time": "0",
                "event_timestamp": t_start.isoformat() + "Z",
                "ingest_timestamp": (t_start + timedelta(seconds=2)).isoformat() + "Z",
            })
            records.append({
                **common,
                "acct_session_id": session_id,
                "acct_status_type": "Stop",
                "acct_session_time": str(duration_seconds),
                "event_timestamp": t_stop.isoformat() + "Z",
                "ingest_timestamp": (t_stop + timedelta(seconds=2)).isoformat() + "Z",
            })

        # 2. Tiêm lỗi/nghiệp vụ — inject_conflicts()
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
            "event_timestamp", "ingest_timestamp", "msisdn", "imsi", "imei",
            "rat_type", "framed_ip", "nas_ip", "mcc_mnc"
        ]
        
        with open(self.config.output, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers, extrasaction='ignore')
            writer.writeheader()
            for rec in records:
                writer.writerow(rec)
                
        print(f" Simulation finished completely! {len(records)} records saved into file: {self.config.output}")

    def _stream_to_kafka(self, records: List[Dict[str, Any]]):
        """Dự phòng cấu hình Stream trực tiếp vào Kafka Broker (Sẽ kích hoạt ở Phase 4)"""
        print(f" Mock streaming {len(records)} records directly into Kafka topic '{self.config.kafka_topic}'...")
        
        print(" Stream mock completed successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RADIUS Accounting Stream Log Simulator CLI Engine")
    parser.add_argument("--records", type=int, default=2_000_000)
    parser.add_argument("--subscribers", type=int, default=100_000)
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--duplicate-rate", type=float, default=0.03)
    parser.add_argument("--late-arrival-rate", type=float, default=0.05)
    parser.add_argument("--invalid-imei-rate", type=float, default=0.02)
    parser.add_argument("--conflict-rate", type=float, default=0.01)
    parser.add_argument("--missing-field-rate", type=float, default=0.005)
    parser.add_argument("--output", type=str, default="data/radius_log.csv")
    parser.add_argument("--kafka", action="store_true")
    parser.add_argument("--kafka-topic", type=str, default="radius.raw")
    
    args = parser.parse_args()
    
    config = SimulatorConfig(
        records=args.records, subscribers=args.subscribers, days=args.days, seed=args.seed,
        duplicate_rate=args.duplicate_rate, late_arrival_rate=args.late_arrival_rate,
        invalid_imei_rate=args.invalid_imei_rate, 
        missing_field_rate=args.missing_field_rate, output=args.output,
        kafka=args.kafka, kafka_topic=args.kafka_topic
    )
    
    simulator = RadiusSimulator(config)
    simulator.execute_simulation()