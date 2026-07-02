from typing import Dict


def base_subscriber(index: int) -> Dict[str, str]:
    """
    Công thức DUY NHẤT để sinh cặp (imsi, msisdn) gốc theo index.
    Dùng chung bởi hlr_hss/seed.py VÀ simulator/generator.py.
    Không dùng random — thuần công thức nên không bao giờ lệch giữa 2 bên.
    """
    return {
        "imsi": f"452010{index:09d}",
        "msisdn": f"+8497{index:07d}",
    }


# [FIX Conflict C] Trước đây HLR/HSS seed.py quyết định subscriber nào có
# SIM Swap bằng rng.random() < 0.02 -- một RNG RIÊNG, độc lập hoàn toàn với
# RNG của simulator. Do đó simulator KHÔNG THỂ biết msisdn nào thực sự có
# 2 dòng IMSI trong HLR mock -> imsi "swap" simulator tự bịa ra gần như
# chắc chắn không khớp history thật -> swap_detector luôn trả None (false
# positive 100%, swap_event luôn trống).
#
# Sửa: thay xác suất ngẫu nhiên bằng CÔNG THỨC XÁC ĐỊNH (modulo), đặt ở
# đây (shared) để cả hlr_hss/seed.py và simulator dùng chung, đảm bảo
# luôn đồng bộ 100% mà không cần gọi API qua lại lúc sinh dữ liệu.
SWAP_MODULO = 50  # 1/50 = 2%, khớp tỷ lệ SIM Swap cũ trong seed.py


def has_sim_swap(index: int) -> bool:
    """Subscriber `index` có được HLR/HSS mock seed sẵn 1 bản ghi SIM Swap
    (2 dòng IMSI cho cùng msisdn) hay không -- xác định, không dùng RNG."""
    return index % SWAP_MODULO == 0


def swap_new_imsi_subscriber(index: int, pool_size: int) -> Dict[str, str]:
    """IMSI/MSISDN của bản ghi 'sau swap' cho subscriber gốc tại `index`.
    Chỉ có ý nghĩa khi has_sim_swap(index) == True. Công thức PHẢI khớp
    chính xác với cách hlr_hss/seed.py sinh dòng swap: base_subscriber(pool_size + index)."""
    return base_subscriber(pool_size + index)