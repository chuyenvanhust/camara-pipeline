#simulator\config.py
from dataclasses import dataclass

@dataclass
class SimulatorConfig:
    records: int = 2_000_000
    subscribers: int = 100_000
    days: int = 90
    seed: int = 42
    duplicate_rate: float = 0.0
    late_arrival_rate: float = 0.0
    invalid_imei_rate: float = 0.0
    conflict_rate: float = 0.0
    missing_field_rate: float = 0.0
    sim_swap_rate: float = 0.002
    device_swap_rate: float = 0.002
    output: str = "data/radius_log.csv"
    kafka: bool = False
    kafka_topic: str = "radius.accounting.raw"