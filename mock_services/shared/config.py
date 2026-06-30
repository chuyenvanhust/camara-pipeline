import os

class MockConfig:
    # ITU
    ITU_CC_PATH = os.getenv("ITU_CC_PATH", "mock_services/itu_e164/data/country_codes.csv")
    ITU_OP_PATH = os.getenv("ITU_OP_PATH", "mock_services/itu_e164/data/operator_prefixes.csv")
    
    # HLR
    HLR_DATA_PATH = os.getenv("HLR_DATA_PATH", "mock_services/hlr_hss/data/subscribers.csv")
    
    # GSMA
    GSMA_DATA_PATH = os.getenv("GSMA_DATA_PATH", "mock_services/gsma_tac/data/tac_records.csv")
    
    REFRESH_INTERVAL = 300 # 5 phút