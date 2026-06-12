import pytest
from simulator.config import SimulatorConfig

def test_simulator_config_default_values(base_config):
    assert base_config.seed == 42
    assert base_config.records == 50
    assert base_config.kafka is False
    assert base_config.duplicate_rate == 0.0