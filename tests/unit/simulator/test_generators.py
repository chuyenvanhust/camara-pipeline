import pytest
from simulator.generators import RadiusDataGenerator

def test_luhn_checksum_calculation():
    gen = RadiusDataGenerator(seed=42)
    # 45201012345678 là chuỗi 14 số đầu, số thứ 15 (checksum) phải là 9
    partial_imei = "45201012345678"
    checksum = gen.generate_luhn_checksum(partial_imei)
    assert checksum == "4"

def test_generate_valid_imei_format():
    gen = RadiusDataGenerator(seed=42)
    gen._fallback_tac_pool()  # Nạp pool dự phòng để test độc lập offline
    
    imei = gen.generate_valid_imei()
    assert len(imei) == 15
    assert imei.isdigit()
    
    # Kiểm tra xem IMEI sinh ra có thỏa mãn thuật toán Luhn (tổng checksum % 10 == 0)
    digits = [int(d) for d in imei]
    for i in range(len(digits) - 2, -1, -2):
        digits[i] *= 2
        if digits[i] > 9:
            digits[i] -= 9
    assert sum(digits) % 10 == 0

def test_generate_base_subscriber_determinism():
    gen = RadiusDataGenerator(seed=42)
    sub1 = gen.generate_base_subscriber(5)
    sub2 = gen.generate_base_subscriber(5)
    
    # Đảm bảo tính nhất quán (Deterministic) với cùng một index đầu vào
    assert sub1["imsi"] == "452010000000005"
    assert sub1["msisdn"] == "+84970000005"
    assert sub1 == sub2