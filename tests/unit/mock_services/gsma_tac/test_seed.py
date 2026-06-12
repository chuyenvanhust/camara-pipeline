import pytest
import os
from mock_services.gsma_tac.seed import load_tac_csv_to_memory, generate_mock_csv, TAC_IN_MEMORY_DB

def test_seed_load_csv_to_memory_correctness(setup_test_csv_data):
    load_tac_csv_to_memory(file_path=setup_test_csv_data)
    assert "352099" in TAC_IN_MEMORY_DB
    assert TAC_IN_MEMORY_DB["352099"].manufacturer == "Samsung"
    assert len(TAC_IN_MEMORY_DB) == 3

def test_seed_file_not_found_raises_error():
    with pytest.raises(FileNotFoundError):
        load_tac_csv_to_memory(file_path="non_existent_file.csv")

def test_seed_generate_mock_csv():
    temp_file = "tests/unit/mock_services/gsma_tac/temp_gen.csv"
    generate_mock_csv(temp_file, count=10, seed_value=42)
    assert os.path.exists(temp_file)
    os.remove(temp_file)