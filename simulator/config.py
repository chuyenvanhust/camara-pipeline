from dataclasses import dataclass

@dataclass
class SimulatorConfig:
    records: int = 1000
    subscribers: int = 100
    days: int = 90
    seed: int = 42
    duplicate_rate: float = 0.03
    late_arrival_rate: float = 0.05
    invalid_imei_rate: float = 0.02
    conflict_rate: float = 0.01
    missing_field_rate: float = 0.005
    output: str = "data/radius_log.csv"
    kafka: bool = False
    kafka_topic: str = "radius.raw"