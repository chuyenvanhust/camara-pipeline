from typing import Dict


def base_subscriber(index: int) -> Dict[str, str]:
    return {
        "imsi": f"452010{index:09d}",
        "msisdn": f"+8497{index:07d}",
    }


SWAP_MODULO = 50


def has_sim_swap(index: int) -> bool:
    return index % SWAP_MODULO == 0


def swap_new_imsi_subscriber(index: int, pool_size: int) -> Dict[str, str]:
    return base_subscriber(pool_size + index)


# ── Device Swap (Conflict D) — đối xứng với SIM Swap ở trên ────────────────

def _luhn_checksum(number_str: str) -> str:
    digits = [int(d) for d in number_str]
    for i in range(len(digits) - 1, -1, -2):
        val = digits[i] * 2
        digits[i] = val if val < 10 else val - 9
    total = sum(digits)
    return str((10 - (total % 10)) % 10)


DEVICE_SWAP_MODULO = 40  


def base_device_imei(index: int) -> str:
    """IMEI gốc cố định gắn với subscriber `index`. TAC=999999 là namespace
    riêng cho pool này, không đụng TAC pool thật của gsma_tac."""
    partial = f"999999{index:08d}"  # 14 số
    return partial + _luhn_checksum(partial)


def has_device_swap(index: int) -> bool:
    """Subscriber `index` có được HLR/HSS mock seed sẵn lịch sử đổi máy
    (Conflict D) hay không -- xác định, không dùng RNG."""
    return index % DEVICE_SWAP_MODULO == 0


def device_swap_new_imei_subscriber(index: int, pool_size: int) -> str:
    """IMEI 'sau swap'. Công thức PHẢI khớp chính xác với hlr_hss/seed.py."""
    return base_device_imei(pool_size + index)