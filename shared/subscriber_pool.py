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



SWAP_MODULO = 50


def has_sim_swap(index: int) -> bool:
    """Subscriber `index` có được HLR/HSS mock seed sẵn 1 bản ghi SIM Swap
    (2 dòng IMSI cho cùng msisdn) hay không -- xác định, không dùng RNG."""
    return index % SWAP_MODULO == 0


def swap_new_imsi_subscriber(index: int, pool_size: int) -> Dict[str, str]:
    """IMSI/MSISDN của bản ghi 'sau swap' cho subscriber gốc tại `index`.
    Chỉ có ý nghĩa khi has_sim_swap(index) == True. Công thức PHẢI khớp
    chính xác với cách hlr_hss/seed.py sinh dòng swap: base_subscriber(pool_size + index)."""
    return base_subscriber(pool_size + index)